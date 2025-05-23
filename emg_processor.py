# emg_processor.py
import json
import time
import numpy as np
from scipy.signal import welch, detrend, butter, filtfilt
from kafka import KafkaConsumer, KafkaProducer, errors as kafka_errors
from collections import defaultdict, deque
import logging
import datetime
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor

# Import configurations and modules
import config
# import db_writer
from fatigue_analyser import perform_fatigue_analysis_and_alert

# --- Setup Logger ---
# (Moved logger setup to a dedicated function for clarity)
def setup_logging():
    logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
    return logging.getLogger(__name__)

logger = setup_logging()

# --- Derived Processing Parameters ---
FFT_WINDOW_SAMPLES = int(config.SAMPLING_RATE_HZ * config.FFT_WINDOW_SEC)
NPERSEG_WELCH = FFT_WINDOW_SAMPLES
NOVERLAP_WELCH = int(NPERSEG_WELCH * config.FFT_OVERLAP_PERCENT)

# --- State Management (Global within this module) ---
emg_sample_buffer = defaultdict(lambda: deque(maxlen=NPERSEG_WELCH))
# mdf_history stores: (mdf_source_event_timestamp_ms, mdf_value, avg_flink_sent_ts_for_fft_window, avg_source_ts_in_current_fft_window)
mdf_history = defaultdict(lambda: deque(maxlen=int(config.TREND_WINDOW_SEC * (1.0 / config.FFT_WINDOW_SEC) * 2.5))) # Max items for trend window
last_alert_time_mdf = defaultdict(float) # Stores last alert emission timestamp (seconds since epoch)
last_alert_time_mdf_lock = threading.Lock()

# --- Kafka Clients (Global within this module) ---
kafka_consumer = None
kafka_producer = None

# --- Thread Pool Executor (Global within this module) ---
fatigue_analysis_executor = None

# --- Helper Functions (Can be moved to a utils.py if they grow) ---
def butter_bandpass_filter(data, lowcut, highcut, fs, order=config.FILTER_ORDER):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    if high >= 1.0: high = 0.99
    if low <= 0.0: low = 0.01
    if low >= high:
        logger.warning(f"Lowcut {low*nyq:.2f}Hz is >= highcut {high*nyq:.2f}Hz. Skipping filter.")
        return data
    b, a = butter(order, [low, high], btype='band', analog=False)
    y = filtfilt(b, a, data)
    return y

def parse_timestamp_ms(data, field_name="sourceTimestampMs"):
    ts_ms = data.get(field_name)
    if ts_ms is None:
        logger.warning(f"Timestamp field '{field_name}' missing in data: {data}")
        return None
    try:
        return int(ts_ms)
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse timestamp '{ts_ms}' from field '{field_name}': {e}")
        return None

def calculate_mdf_from_psd(frequencies, psd):
    if frequencies is None or psd is None or len(frequencies) == 0 or len(psd) == 0 or len(frequencies) != len(psd):
        logger.warning("Invalid input for MDF calculation from PSD.")
        return np.nan
    total_power = np.sum(psd)
    if total_power < 1e-12:
        logger.debug("Total power too low, cannot calculate MDF.")
        return np.nan
    cumulative_power = np.cumsum(psd)
    try:
        median_freq_index = np.searchsorted(cumulative_power, total_power / 2.0, side='left')
        if median_freq_index >= len(frequencies): median_freq_index = len(frequencies) - 1
        if median_freq_index < 0: return np.nan # Should not happen if total_power > 0
        return frequencies[median_freq_index]
    except IndexError:
        logger.warning("IndexError during MDF calculation from PSD.", exc_info=True)
        return np.nan
    except Exception as e:
        logger.error(f"Unexpected error in calculate_mdf_from_psd: {e}", exc_info=True)
        return np.nan

def initialize_kafka_clients():
    global kafka_consumer, kafka_producer
    max_retries = 3
    retry_delay_sec = 5
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting Kafka Consumer connection (Attempt {attempt + 1}/{max_retries})")
            kafka_consumer = KafkaConsumer(
                config.EMG_MDF_INPUT_TOPIC,
                bootstrap_servers=config.KAFKA_BROKERS,
                group_id=config.CONSUMER_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8', errors='ignore')),
                auto_offset_reset='latest',
                consumer_timeout_ms=2000  # Timeout for poll
            )
            logger.info(f"Kafka Consumer connected to topic: {config.EMG_MDF_INPUT_TOPIC}")

            logger.info(f"Attempting Kafka Producer connection (Attempt {attempt + 1}/{max_retries})")
            kafka_producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5, # Increased retries
                request_timeout_ms=15000, # Timeout for individual requests
                linger_ms=10 # Batch messages for 10ms
            )
            logger.info("Kafka Producer connected.")
            return True
        except kafka_errors.NoBrokersAvailable as e:
            logger.error(f"Kafka connection failed (Attempt {attempt + 1}/{max_retries}): NoBrokersAvailable - {e}. Retrying in {retry_delay_sec}s...")
            if attempt < max_retries - 1:
                time.sleep(retry_delay_sec)
            else:
                logger.error("Max retries reached for Kafka connection.")
                return False
        except Exception as e:
            logger.error(f"Failed to create Kafka clients (Attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
            if kafka_consumer: kafka_consumer.close()
            if kafka_producer: kafka_producer.close() # Ensure producer is also closed on partial failure
            return False
    return False

def main_processing_loop():
    global kafka_consumer, kafka_producer, fatigue_analysis_executor

    if not initialize_kafka_clients():
        logger.critical("Failed to initialize Kafka clients. Exiting.")
        return

    # db_writer.init_db_config(
    #     config.DB_HOST, config.DB_PORT, config.DB_NAME,
    #     config.DB_USER, config.DB_PASSWORD, config.MDF_TABLE_NAME
    # )

    fatigue_analysis_executor = ThreadPoolExecutor(
        max_workers=config.FATIGUE_ANALYSIS_WORKERS,
        thread_name_prefix='fatigue_worker'
    )

    logger.info("Starting EMG MDF fatigue processing loop...")
    try:
        while True:
            message_pack = kafka_consumer.poll(timeout_ms=1000) # Poll with a timeout
            if not message_pack:
                # logger.debug("No messages received from Kafka, continuing poll.")
                time.sleep(0.05) # Small sleep if no messages to prevent busy-looping 100% CPU on poll
                continue

            for tp, records in message_pack.items():
                for record in records:
                    try:
                        data = record.value
                        python_receipt_timestamp_ms = int(time.time() * 1000)
                        logger.debug(f"Received msg on {tp.topic}: Offset {record.offset}")

                        thingid = data.get("thingid")
                        source_timestamp_ms = parse_timestamp_ms(data, "sourceTimestampMs")
                        flink_sent_timestamp_ms = parse_timestamp_ms(data, "flinkSentTimestampMs")
                        muscle_name = data.get("muscle")
                        value_uV_str = data.get("value")

                        if not all([thingid, source_timestamp_ms is not None,
                                    flink_sent_timestamp_ms is not None,
                                    muscle_name, value_uV_str is not None]):
                            logger.warning(f"Skipping record with missing core fields: {data}")
                            continue

                        if muscle_name not in config.MUSCLES_TO_MONITOR_FOR_MDF:
                            logger.debug(f"Skipping muscle {muscle_name} not in MDF monitor list.")
                            continue

                        try:
                            value_uV = float(value_uV_str)
                            if np.isnan(value_uV) or np.isinf(value_uV):
                                logger.debug(f"Skipping NaN/Inf EMG value for {thingid}-{muscle_name}")
                                continue
                        except (ValueError, TypeError):
                            logger.warning(f"Could not convert EMG value '{value_uV_str}' to float for {thingid}-{muscle_name}")
                            continue

                        key = (thingid, muscle_name)
                        emg_sample_buffer[key].append((source_timestamp_ms, flink_sent_timestamp_ms, value_uV))

                        if len(emg_sample_buffer[key]) == NPERSEG_WELCH:
                            current_fft_window_data_tuples = list(emg_sample_buffer[key])
                            signal_segment = np.array([item[2] for item in current_fft_window_data_tuples])

                            source_timestamps_in_fft_window = [item[0] for item in current_fft_window_data_tuples]
                            flink_sent_timestamps_in_fft_window = [item[1] for item in current_fft_window_data_tuples]

                            mdf_calc_source_timestamp_ms = source_timestamps_in_fft_window[-1]
                            avg_source_ts_for_this_fft_window = statistics.mean(source_timestamps_in_fft_window) if source_timestamps_in_fft_window else mdf_calc_source_timestamp_ms
                            avg_flink_sent_ts_for_this_fft_window = statistics.mean(flink_sent_timestamps_in_fft_window) if flink_sent_timestamps_in_fft_window else flink_sent_timestamp_ms

                            if config.APPLY_PRE_FILTERING:
                                signal_segment = butter_bandpass_filter(signal_segment, config.LOWCUT_HZ, config.HIGHCUT_HZ, config.SAMPLING_RATE_HZ)

                            signal_detrended = detrend(signal_segment, type='linear')

                            try:
                                frequencies, psd = welch(signal_detrended, fs=config.SAMPLING_RATE_HZ, window=config.WINDOW_TYPE_WELCH,
                                                         nperseg=NPERSEG_WELCH, noverlap=NOVERLAP_WELCH, scaling='density')
                            except Exception as e_welch:
                                logger.error(f"Error in Welch calculation for key {key}: {e_welch}", exc_info=True)
                                continue # Skip this segment

                            mdf = calculate_mdf_from_psd(frequencies, psd)

                            if not np.isnan(mdf):
                                logger.debug(f"MDF Calculated: Key={key}, MDF_Src_Time={datetime.datetime.fromtimestamp(mdf_calc_source_timestamp_ms/1000.0, tz=datetime.timezone.utc)}, MDF={mdf:.2f} Hz")

                                mdf_history[key].append((mdf_calc_source_timestamp_ms, mdf, avg_flink_sent_ts_for_this_fft_window, avg_source_ts_for_this_fft_window))

                                # Send to visualization topic (main thread)
                                mdf_viz_payload = {
                                    "thingId": thingid, "muscle": muscle_name,
                                    "sourceTimestampMs": mdf_calc_source_timestamp_ms,
                                    "mdfValue": round(mdf, 4),
                                    "avgFlinkSentTimestampForWindowDataMs": round(avg_flink_sent_ts_for_this_fft_window),
                                    "avgSourceTimestampForWindowDataMs": round(avg_source_ts_for_this_fft_window)
                                }
                                try:
                                    kafka_producer.send(config.MDF_VISUALIZATION_TOPIC, value=mdf_viz_payload)
                                except Exception as e_prod_viz:
                                    logger.error(f"Error sending MDF to visualization topic for {key}: {e_prod_viz}", exc_info=True)

                                # Save MDF value to database (main thread)
                                # db_writer.save_mdf_value_to_db(thingid, muscle_name, mdf_calc_source_timestamp_ms, round(mdf, 4))

                                # Prune old mdf_history (main thread)
                                trend_window_start_ts = mdf_calc_source_timestamp_ms - (config.TREND_WINDOW_SEC * 1000)
                                while mdf_history[key] and mdf_history[key][0][0] < trend_window_start_ts:
                                    mdf_history[key].popleft()

                                # Check conditions and submit fatigue analysis to worker thread
                                main_thread_current_time_s = time.time()
                                should_submit_analysis = False
                                with last_alert_time_mdf_lock:
                                    if main_thread_current_time_s >= last_alert_time_mdf.get(key, 0) + config.ALERT_COOLDOWN_SEC:
                                        if len(mdf_history[key]) >= config.MIN_MDF_POINTS_FOR_TREND:
                                            should_submit_analysis = True
                                    else:
                                        logger.debug(f"Key {key}: In MDF alert cooldown (main check). Last alert: {last_alert_time_mdf.get(key,0):.2f}, Now: {main_thread_current_time_s:.2f}")

                                if should_submit_analysis:
                                    history_copy_for_worker = list(mdf_history[key])
                                    fatigue_analysis_executor.submit(
                                        perform_fatigue_analysis_and_alert,
                                        key, history_copy_for_worker, mdf_calc_source_timestamp_ms,
                                        kafka_producer, last_alert_time_mdf, last_alert_time_mdf_lock
                                    )
                                    logger.debug(f"Key {key}: Submitted trend analysis to worker with {len(history_copy_for_worker)} points.")

                    except json.JSONDecodeError:
                        logger.warning(f"Could not decode JSON from record value: {record.value}", exc_info=True)
                    except Exception as e_record:
                        logger.error(f"General error processing record: {e_record}", exc_info=True)
                        # This will catch errors from within the record processing loop
                        # and allow the consumer to continue with the next record.

    except KeyboardInterrupt:
        logger.info("MDF Processor: Shutdown signal (KeyboardInterrupt) received.")
    except Exception as e_main_loop:
        logger.critical(f"MDF Processor: Critical error in main loop: {e_main_loop}", exc_info=True)
    finally:
        shutdown_gracefully()

def shutdown_gracefully():
    global kafka_consumer, kafka_producer, fatigue_analysis_executor
    logger.info("MDF Processor: Initiating graceful shutdown...")

    if fatigue_analysis_executor:
        logger.info("MDF Processor: Shutting down fatigue_analysis_executor...")
        fatigue_analysis_executor.shutdown(wait=True) # Wait for pending tasks
        logger.info("MDF Processor: Fatigue_analysis_executor shut down.")

    if kafka_consumer:
        logger.info("MDF Processor: Closing Kafka Consumer...")
        try:
            kafka_consumer.close() # Autocommit is default, or manage offsets
            logger.info("MDF Processor: Kafka Consumer closed.")
        except Exception as e_con_close:
            logger.error(f"Error closing Kafka consumer: {e_con_close}", exc_info=True)

    if kafka_producer:
        logger.info("MDF Processor: Flushing and closing Kafka Producer...")
        try:
            kafka_producer.flush(timeout=10) # Attempt to flush remaining messages
            kafka_producer.close(timeout=10)
            logger.info("MDF Processor: Kafka Producer closed.")
        except Exception as e_prod_close:
            logger.error(f"Error closing Kafka producer: {e_prod_close}", exc_info=True)

    # db_writer.close_db_connection()
    logger.info("MDF Processor: Shutdown complete. Exiting.")


if __name__ == "__main__":
    main_processing_loop()