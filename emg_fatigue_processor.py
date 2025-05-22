import json
import time
import numpy as np
from scipy.signal import welch, detrend, butter, filtfilt # Using filtfilt for zero-phase filtering
from scipy.stats import linregress
from kafka import KafkaConsumer, KafkaProducer, errors as kafka_errors
from collections import defaultdict, deque
import logging
import datetime
import threading # For potential future scalability if needed
import statistics # For mean calculation
from db_writer import save_mdf_value_to_db, close_db_connection

# --- Configuration ---
KAFKA_BROKERS = ['192.168.50.105:9092', '192.168.50.105:9093', '192.168.50.105:9094']
EMG_MDF_INPUT_TOPIC = "th_emg_for_mdf_processing" # Topic Flink sends specific muscle data to
FATIGUE_ALERT_TOPIC = 'th_emg_mdf_fatigue_alerts' # Standardized alert topic name
MDF_VISUALIZATION_TOPIC = 'th_emg_mdf_visualization_data' # NEW: Topic for individual MDF values
CONSUMER_GROUP_ID = 'python-emg-mdf-processor-v4'

SAMPLING_RATE_HZ = 100.0  # Hz - MUST match the source data rate
FFT_WINDOW_SEC = 1.0      # Duration of window for FFT/MDF
FFT_WINDOW_SAMPLES = int(SAMPLING_RATE_HZ * FFT_WINDOW_SEC)
FFT_OVERLAP_PERCENT = 0.5 # 50% overlap for Welch is common
NPERSEG_WELCH = FFT_WINDOW_SAMPLES
NOVERLAP_WELCH = int(NPERSEG_WELCH * FFT_OVERLAP_PERCENT)
WINDOW_TYPE_WELCH = 'hann'

# Optional Bandpass Pre-filtering settings
FILTER_ORDER = 4
LOWCUT_HZ = 20.0
HIGHCUT_HZ = 40.0 
APPLY_PRE_FILTERING = False

TREND_WINDOW_SEC = 60
MIN_MDF_POINTS_FOR_TREND = 10 
MDF_SLOPE_FATIGUE_THRESHOLD = -0.10 
REGRESSION_R_SQUARED_THRESHOLD = 0.25 
ALERT_COOLDOWN_SEC = 60

MUSCLES_TO_MONITOR_FOR_MDF = {
    "trapezius_right",
    "deltoids_right",
    "latissimus_right" # Note: Ensure this matches what Flink sends. Often "latissimus_dorsi_right"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# --- State Management ---
emg_sample_buffer = defaultdict(lambda: deque(maxlen=NPERSEG_WELCH)) 
# mdf_history stores: (mdf_source_event_timestamp_ms, mdf_value, avg_flink_sent_ts_for_fft_window, avg_source_ts_in_current_fft_window)
mdf_history = defaultdict(lambda: deque(maxlen=int(TREND_WINDOW_SEC * (1.0/FFT_WINDOW_SEC) * 2.5)))
last_alert_time_mdf = defaultdict(float)

# --- Helper Functions ---
def butter_bandpass_filter(data, lowcut, highcut, fs, order=FILTER_ORDER):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    if high >= 1.0: high = 0.99 
    if low <= 0.0: low = 0.01  
    if low >= high:
        logger.warning(f"Lowcut {low*nyq}Hz is >= highcut {high*nyq}Hz. Skipping filter.")
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
        if median_freq_index >= len(frequencies):
            median_freq_index = len(frequencies) - 1
        if median_freq_index < 0 : return np.nan
        return frequencies[median_freq_index]
    except IndexError:
        logger.warning("IndexError during MDF calculation from PSD.", exc_info=True)
        return np.nan

# --- Kafka Clients ---
consumer = None
producer = None

def initialize_kafka_clients_mdf():
    global consumer, producer
    max_retries = 3
    retry_delay_sec = 5
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect Kafka Consumer (Attempt {attempt + 1}/{max_retries})")
            consumer = KafkaConsumer(
                EMG_MDF_INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=CONSUMER_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8', errors='ignore')),
                auto_offset_reset='latest',
                consumer_timeout_ms=2000
            )
            logger.info(f"Kafka Consumer connected to topic: {EMG_MDF_INPUT_TOPIC}")

            logger.info(f"Attempting to connect Kafka Producer (Attempt {attempt + 1}/{max_retries})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3 
            )
            logger.info("Kafka Producer connected.")
            return True
        except kafka_errors.NoBrokersAvailable as e:
            logger.error(f"Kafka connection failed (Attempt {attempt + 1}/{max_retries}): NoBrokersAvailable - {e}. Retrying in {retry_delay_sec}s...")
            if attempt < max_retries - 1:
                time.sleep(retry_delay_sec)
            else:
                logger.error("Max retries reached for Kafka connection. Exiting.")
                return False
        except Exception as e:
            logger.error(f"Failed to create Kafka clients (Attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
            if consumer: consumer.close()
            return False
    return False

# --- Main Processing Loop ---
def process_emg_mdf_fatigue():
    if not initialize_kafka_clients_mdf():
        return

    logger.info("Starting EMG MDF fatigue processing loop...")
    try:
        while True:
            message_pack = consumer.poll(timeout_ms=1000)
            if not message_pack:
                time.sleep(0.05) 
                continue

            for tp, records in message_pack.items():
                for record in records:
                    try:
                        data = record.value
                        python_receipt_timestamp_ms = int(time.time() * 1000) # For internal Python processing delay if needed
                        logger.debug(f"Received msg on {tp.topic}: {data}")

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
                        
                        if muscle_name not in MUSCLES_TO_MONITOR_FOR_MDF:
                            logger.debug(f"Skipping muscle {muscle_name} not in MDF monitor list.")
                            continue

                        try:
                            value_uV = float(value_uV_str)
                            if np.isnan(value_uV) or np.isinf(value_uV): continue
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

                            if APPLY_PRE_FILTERING:
                                signal_segment = butter_bandpass_filter(signal_segment, LOWCUT_HZ, HIGHCUT_HZ, SAMPLING_RATE_HZ)
                            
                            signal_detrended = detrend(signal_segment, type='linear')
                            
                            try:
                                frequencies, psd = welch(signal_detrended, fs=SAMPLING_RATE_HZ, window=WINDOW_TYPE_WELCH,
                                                         nperseg=NPERSEG_WELCH, noverlap=NOVERLAP_WELCH, scaling='density')
                            except Exception as e:
                                logger.error(f"Error in Welch for key {key}: {e}", exc_info=True)
                                continue 

                            mdf = calculate_mdf_from_psd(frequencies, psd)

                            if not np.isnan(mdf):
                                logger.debug(f"MDF Calculated: Key={key}, MDF_Src_Time={datetime.datetime.fromtimestamp(mdf_calc_source_timestamp_ms/1000.0)}, MDF={mdf:.2f} Hz")
                                
                                mdf_history[key].append((mdf_calc_source_timestamp_ms, mdf, avg_flink_sent_ts_for_this_fft_window, avg_source_ts_for_this_fft_window))
                                
                                # --- SEND INDIVIDUAL MDF VALUE FOR VISUALIZATION ---
                                mdf_viz_payload = {
                                    "thingId": thingid,
                                    "muscle": muscle_name,
                                    "sourceTimestampMs": mdf_calc_source_timestamp_ms, # Timestamp of this MDF point
                                    "mdfValue": round(mdf, 2),
                                    # Optional: include avg Flink sent time for data in this MDF's window
                                    "avgFlinkSentTimestampForWindowDataMs": round(avg_flink_sent_ts_for_this_fft_window),
                                    "avgSourceTimestampForWindowDataMs": round(avg_source_ts_for_this_fft_window)
                                }
                                try:
                                    producer.send(MDF_VISUALIZATION_TOPIC, value=mdf_viz_payload)
                                    # producer.flush() # Flush frequently if near real-time viz is critical and volume is low
                                except Exception as e_prod_viz:
                                    logger.error(f"Error sending MDF to visualization topic for {key}: {e_prod_viz}")
                                # --- END SEND INDIVIDUAL MDF VALUE ---

                                save_mdf_value_to_db(thingid, muscle_name, mdf_calc_source_timestamp_ms, round(mdf, 4)) # Using more precision for DB

                                trend_window_start_ts = mdf_calc_source_timestamp_ms - (TREND_WINDOW_SEC * 1000)
                                while mdf_history[key] and mdf_history[key][0][0] < trend_window_start_ts:
                                    mdf_history[key].popleft()
                                
                                current_sys_time_s_py = time.time() 
                                if current_sys_time_s_py < last_alert_time_mdf.get(key, 0) + ALERT_COOLDOWN_SEC:
                                    logger.debug(f"Key {key}: In MDF alert cooldown period.")
                                    continue

                                if len(mdf_history[key]) >= MIN_MDF_POINTS_FOR_TREND:
                                    history_points_for_trend = list(mdf_history[key])
                                    first_mdf_source_ts_in_trend = history_points_for_trend[0][0]
                                    
                                    times_rel_sec_trend = np.array([(item[0] - first_mdf_source_ts_in_trend) / 1000.0 for item in history_points_for_trend])
                                    mdfs_values_trend = np.array([item[1] for item in history_points_for_trend])
                                    
                                    # For overall delay calculation for the alert
                                    source_timestamps_of_mdf_data_in_trend = [item[3] for item in history_points_for_trend] # avg_source_ts_for_fft_window
                                    flink_sent_timestamps_of_mdf_data_in_trend = [item[2] for item in history_points_for_trend] # avg_flink_sent_ts_for_fft_window


                                    if len(times_rel_sec_trend) < 2: continue

                                    try:
                                        slope, intercept, r_value, p_value, std_err = linregress(times_rel_sec_trend, mdfs_values_trend)
                                        if np.isnan(slope) or np.isnan(r_value):
                                             logger.warning(f"Linreg resulted in NaN for key {key}. Slope={slope}, R={r_value}")
                                             continue
                                        r_squared = r_value**2

                                        logger.info(f"MDF Trend: Key={key}, Slope={slope:.4f} Hz/s, R^2={r_squared:.3f}, Points={len(mdfs_values_trend)}")

                                        if slope < MDF_SLOPE_FATIGUE_THRESHOLD and r_squared >= REGRESSION_R_SQUARED_THRESHOLD:
                                            python_alert_emission_time_ms = int(time.time() * 1000) 
                                            
                                            alert_trigger_timestamp_py = mdf_calc_source_timestamp_ms 
                                            
                                            avg_source_ts_of_data_in_trend_overall = statistics.mean(source_timestamps_of_mdf_data_in_trend) if source_timestamps_of_mdf_data_in_trend else alert_trigger_timestamp_py
                                            avg_mdf_data_age_in_trend_ms_py = int(alert_trigger_timestamp_py - avg_source_ts_of_data_in_trend_overall)
                                            
                                            avg_flink_sent_ts_for_trend_mds_overall = statistics.mean(flink_sent_timestamps_of_mdf_data_in_trend) if flink_sent_timestamps_of_mdf_data_in_trend else 0
                                            flink_pipeline_delay_for_trend_data_ms = 0
                                            if avg_flink_sent_ts_for_trend_mds_overall > 0 and avg_source_ts_of_data_in_trend_overall > 0:
                                                flink_pipeline_delay_for_trend_data_ms = int(avg_flink_sent_ts_for_trend_mds_overall - avg_source_ts_of_data_in_trend_overall)
                                            
                                            python_internal_processing_delay_ms = python_alert_emission_time_ms - alert_trigger_timestamp_py

                                            alert_payload = {
                                                "thingId": thingid,
                                                "alertTriggerTimestamp": alert_trigger_timestamp_py,
                                                "feedbackType": "emgMdfFatigueAlert", 
                                                "muscle": muscle_name,
                                                "severity": "HIGH", 
                                                "reason": "Decreasing MDF Trend Detected (Python)",
                                                "mdfSlopeHzPerSec": round(slope, 4),
                                                "regressionRSquared": round(r_squared, 3),
                                                "trendWindowSecondsUsed": int(round((mdf_calc_source_timestamp_ms - first_mdf_source_ts_in_trend) / 1000.0)),
                                                "numberOfMdfPointsInTrend": len(mdf_history[key]),
                                                "mdfSlopeThreshold": MDF_SLOPE_FATIGUE_THRESHOLD,
                                                "rSquaredThreshold": REGRESSION_R_SQUARED_THRESHOLD,
                                                "lastMdfValue": round(mdfs_values_trend[-1], 2),
                                                "averageMdfDataAgeInTrendMs": avg_mdf_data_age_in_trend_ms_py,
                                                "estimatedFlinkPipelineDelayMs": flink_pipeline_delay_for_trend_data_ms,
                                                "pythonProcessingTimeForAlertMs": python_internal_processing_delay_ms,
                                                "pythonAlertSentTimestampMs": python_alert_emission_time_ms
                                            }
                                            producer.send(FATIGUE_ALERT_TOPIC, value=alert_payload)
                                            producer.flush() 
                                            logger.warning(f"PYTHON MDF FATIGUE ALERT SENT: Key={key} | Alert: {json.dumps(alert_payload)}")
                                            last_alert_time_mdf[key] = current_sys_time_s_py
                                    except ValueError as ve:
                                         logger.warning(f"Linreg failed for key {key}: {ve}. Data points: {len(times_rel_sec_trend)}")
                                    except Exception as ex_trend:
                                        logger.error(f"Error in trend analysis for key {key}: {ex_trend}", exc_info=True)
                    except json.JSONDecodeError:
                        logger.warning(f"Could not decode JSON from record value: {record.value}")
                    except Exception as e_record:
                        logger.error(f"General error processing record for MDF: {e_record}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("MDF Processor: Shutdown signal received.")
    except Exception as e_main_loop:
        logger.critical(f"MDF Processor: Critical error in main loop: {e_main_loop}", exc_info=True)
    finally:
        logger.info("MDF Processor: Closing Kafka clients and DB connection.")
        if consumer: 
            try: consumer.close(autocommit=False)
            except Exception as e_con_close: logger.error(f"Error closing consumer: {e_con_close}")
        if producer:
            try: 
                producer.flush(timeout=5) 
                producer.close(timeout=5)
            except Exception as e_prod_close: logger.error(f"Error closing producer: {e_prod_close}")
        close_db_connection() # NEW: Close DB connection on exit
        logger.info("MDF Processor: Clients and DB connection closed. Exiting.")

if __name__ == "__main__":
    process_emg_mdf_fatigue()