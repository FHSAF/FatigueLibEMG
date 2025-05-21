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

# --- Configuration ---
KAFKA_BROKERS = ['192.168.50.105:9092', '192.168.50.105:9093', '192.168.50.105:9094']
EMG_MDF_INPUT_TOPIC = "th_emg_for_mdf_processing" # Topic Flink sends specific muscle data to
FATIGUE_ALERT_TOPIC = 'th_emg_mdf_fatigue_alerts' # Standardized alert topic name
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
HIGHCUT_HZ = 40.0 # MDF for fatigue is expected in lower frequencies, but sEMG power goes higher.
                 # Keep a reasonable upper range for general sEMG, Welch will focus on relevant bands.
                 # The paper by Corvini & Conforto (sensors-22-06360) simulated fatigue with fh=40Hz.
                 # Cifrek et al. (1-s2.0-S0268003309000254-main.pdf) uses a broader acquisition bandwidth for EMG.
                 # If data from Flink is already well-filtered for the EMG signal itself (e.g. 20-450Hz),
                 # this pre-filtering here might be redundant or could be narrowed.
APPLY_PRE_FILTERING = False # Set to True to enable bandpass filtering here

TREND_WINDOW_SEC = 60
MIN_MDF_POINTS_FOR_TREND = 10 # Minimum MDF values needed to attempt regression
MDF_SLOPE_FATIGUE_THRESHOLD = -0.10 # Hz/sec - Highly sensitive, NEEDS CALIBRATION!
REGRESSION_R_SQUARED_THRESHOLD = 0.25 # Min R^2 for trend significance - NEEDS CALIBRATION!
ALERT_COOLDOWN_SEC = 60

# This list is a safeguard if Flink somehow sends other muscles.
# Primary filtering should happen in Flink before sending to EMG_MDF_INPUT_TOPIC.
MUSCLES_TO_MONITOR_FOR_MDF = {
    "trapezius_right",
    "deltoids_right",
    "latissimus_right" 
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# --- State Management ---
# Key: tuple (thingid, muscle_name)
# Value: deque containing raw uV samples for Welch method input
emg_sample_buffer = defaultdict(lambda: deque(maxlen=NPERSEG_WELCH)) # Buffer for one segment for Welch

# Key: tuple (thingid, muscle_name)
# Value: deque containing (timestamp_ms_of_mdf_calc, mdf_value)
mdf_history = defaultdict(lambda: deque(maxlen=int(TREND_WINDOW_SEC * (1.0/FFT_WINDOW_SEC) * 2.5))) # *2.5 for safety margin

last_alert_time_mdf = defaultdict(float) # Renamed to avoid conflict if combined with RMS script

# --- Helper Functions ---
def butter_bandpass_filter(data, lowcut, highcut, fs, order=FILTER_ORDER):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    if high >= 1.0: high = 0.99 # Ensure high is less than 1.0 for nyquist
    if low <= 0.0: low = 0.01  # Ensure low is greater than 0.0
    if low >= high:
        logger.warning(f"Lowcut {low*nyq}Hz is >= highcut {high*nyq}Hz. Skipping filter.")
        return data
    b, a = butter(order, [low, high], btype='band', analog=False)
    y = filtfilt(b, a, data) # zero-phase filter
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
    if total_power < 1e-12: # Increased threshold slightly
        logger.debug("Total power too low, cannot calculate MDF.")
        return np.nan
    cumulative_power = np.cumsum(psd)
    try:
        median_freq_index = np.searchsorted(cumulative_power, total_power / 2.0, side='left')
        if median_freq_index >= len(frequencies):
            median_freq_index = len(frequencies) - 1
        if median_freq_index < 0 : return np.nan # Should not happen
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
                retries=3 # Producer internal retries
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
                time.sleep(0.05) # Shorter sleep if no messages for responsiveness
                continue

            for tp, records in message_pack.items():
                for record in records:
                    try:
                        data = record.value
                        logger.debug(f"Received msg on {tp}: {data}")

                        thingid = data.get("thingid")
                        timestamp_ms = parse_timestamp_ms(data) # Uses "timestamp_ms" field
                        muscle_name = data.get("muscle")
                        value_uV_str = data.get("value")

                        if not all([thingid, timestamp_ms is not None, muscle_name, value_uV_str is not None]):
                            logger.warning(f"Skipping record with missing fields: {data}")
                            continue
                        
                        if muscle_name not in MUSCLES_TO_MONITOR_FOR_MDF:
                            continue

                        try:
                            value_uV = float(value_uV_str)
                            if np.isnan(value_uV) or np.isinf(value_uV): continue
                        except (ValueError, TypeError): continue
                        
                        key = (thingid, muscle_name)
                        emg_sample_buffer[key].append(value_uV)

                        if len(emg_sample_buffer[key]) == NPERSEG_WELCH:
                            signal_segment = np.array(list(emg_sample_buffer[key]))

                            if APPLY_PRE_FILTERING:
                                signal_segment = butter_bandpass_filter(signal_segment, LOWCUT_HZ, HIGHCUT_HZ, SAMPLING_RATE_HZ)
                            
                            signal_detrended = detrend(signal_segment, type='linear')
                            
                            try:
                                frequencies, psd = welch(signal_detrended, fs=SAMPLING_RATE_HZ, window=WINDOW_TYPE_WELCH,
                                                         nperseg=NPERSEG_WELCH, noverlap=NOVERLAP_WELCH, scaling='density')
                            except Exception as e:
                                logger.error(f"Error in Welch for key {key}: {e}", exc_info=True)
                                continue # Skip this MDF calculation

                            mdf = calculate_mdf_from_psd(frequencies, psd)

                            if not np.isnan(mdf):
                                # Timestamp for this MDF value is the timestamp of the *last* sample in the window
                                mdf_calc_timestamp_ms = timestamp_ms 
                                logger.debug(f"MDF Calculated: Key={key}, MDF_Time={datetime.datetime.fromtimestamp(mdf_calc_timestamp_ms/1000.0)}, MDF={mdf:.2f} Hz")
                                mdf_history[key].append((mdf_calc_timestamp_ms, mdf))

                                # Prune old MDF history based on the current MDF calculation time
                                trend_window_start_ts = mdf_calc_timestamp_ms - (TREND_WINDOW_SEC * 1000)
                                while mdf_history[key] and mdf_history[key][0][0] < trend_window_start_ts:
                                    mdf_history[key].popleft()
                                
                                current_sys_time_sec = time.time()
                                if current_sys_time_sec < last_alert_time_mdf.get(key, 0) + ALERT_COOLDOWN_SEC:
                                    logger.debug(f"Key {key}: In MDF alert cooldown period.")
                                    continue

                                if len(mdf_history[key]) >= MIN_MDF_POINTS_FOR_TREND:
                                    history_points = list(mdf_history[key])
                                    first_mdf_ts_in_history = history_points[0][0]
                                    
                                    # Relative time for regression (in seconds)
                                    times_rel_sec = np.array([(ts - first_mdf_ts_in_history) / 1000.0 for ts, _ in history_points])
                                    mdfs_hist_values = np.array([mdf_val for _, mdf_val in history_points])

                                    if len(times_rel_sec) < 2: continue

                                    try:
                                        slope, intercept, r_value, p_value, std_err = linregress(times_rel_sec, mdfs_hist_values)
                                        if np.isnan(slope) or np.isnan(r_value):
                                             logger.warning(f"Linreg resulted in NaN for key {key}. Slope={slope}, R={r_value}")
                                             continue
                                        r_squared = r_value**2

                                        logger.info(f"MDF Trend: Key={key}, Slope={slope:.4f} Hz/s, R^2={r_squared:.3f}, Points={len(times_rel_sec)}")

                                        if slope < MDF_SLOPE_FATIGUE_THRESHOLD and r_squared >= REGRESSION_R_SQUARED_THRESHOLD:
                                            logger.warning(f"PYTHON MDF FATIGUE ALERT: Key={key} | Slope: {slope:.3f}, R^2: {r_squared:.3f}")
                                            
                                            alert_payload = {
                                                "thingId": thingid,
                                                "alertTriggerTimestamp": mdf_calc_timestamp_ms,
                                                "feedbackType": "emgMdfFatigueAlert", # Differentiate from RMS
                                                "muscle": muscle_name,
                                                "severity": "HIGH",
                                                "reason": "Decreasing MDF Trend Detected",
                                                "mdfSlopeHzPerSec": round(slope, 4),
                                                "regressionRSquared": round(r_squared, 3),
                                                "trendWindowSecondsUsed": int(round((mdf_calc_timestamp_ms - first_mdf_ts_in_history) / 1000.0)),
                                                "numberOfMdfPointsInTrend": len(mdf_history[key]),
                                                "mdfSlopeThreshold": MDF_SLOPE_FATIGUE_THRESHOLD,
                                                "rSquaredThreshold": REGRESSION_R_SQUARED_THRESHOLD,
                                                "lastMdfValue": round(mdfs_hist_values[-1], 2)
                                            }
                                            producer.send(FATIGUE_ALERT_TOPIC, value=alert_payload)
                                            producer.flush()
                                            logger.info(f"Sent MDF fatigue alert for {key} to {FATIGUE_ALERT_TOPIC}")
                                            last_alert_time_mdf[key] = current_sys_time_sec
                                            # mdf_history[key].clear() # Optional: Reset trend after alert
                                    except ValueError as ve:
                                         logger.warning(f"Linreg failed for key {key}: {ve}. Data points: {len(times_rel_sec)}")
                                    except Exception as ex:
                                        logger.error(f"Error in trend analysis for {key}: {ex}", exc_info=True)
                            # After processing, shift buffer for next Welch segment if using overlap
                            # If NPERSEG is 100 and NOVERLAP is 50, we want to keep last 50 samples for next window.
                            # This is implicitly handled by how we extract 'signal_segment' if emg_sample_buffer
                            # continuously gets data. However, for strict Welch, we'd process fixed blocks.
                            # The current emg_sample_buffer with maxlen=NPERSEG_WELCH means each MDF
                            # is from a distinct (tumbling) window of raw samples.
                            # If you want overlapping Welch segments from a continuous stream, the buffer logic needs to be more complex
                            # or you pass larger chunks to a function that internally does the overlapping segmentation for Welch.
                            # For simplicity here, this calculates MDF on NPERSEG_WELCH samples when that many *new* samples for that muscle arrive.

                    except json.JSONDecodeError:
                        logger.warning(f"Could not decode JSON from record value: {record.value}")
                    except Exception as e:
                        logger.error(f"General error processing record for MDF: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("MDF Processor: Shutdown signal received.")
    except Exception as e:
        logger.critical(f"MDF Processor: Critical error in main loop: {e}", exc_info=True)
    finally:
        logger.info("MDF Processor: Closing Kafka clients.")
        if consumer: consumer.close(autocommit=False) # autocommit=False for graceful exit
        if producer:
            try: producer.flush(timeout=10)
            except Exception as e: logger.error(f"MDF Processor: Error flushing Kafka producer: {e}")
            producer.close(timeout=10)
        logger.info("MDF Processor: Clients closed. Exiting.")

if __name__ == "__main__":
    process_emg_mdf_fatigue()
