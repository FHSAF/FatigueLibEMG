import json
import time
import numpy as np
from scipy.signal import welch, detrend # For PSD calculation
from scipy.stats import linregress # For trend analysis
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict, deque
import logging
import datetime
import threading # For potential future multi-threading if needed

# --- Configuration ---
# Kafka Settings
KAFKA_BROKERS = ['192.168.50.105:9092', '192.168.50.105:9093', '192.168.50.105:9094'] # Replace with your brokers
RAW_EMG_TOPICS = [
    'k_myontech_shirt01_emg', # Left Arm
    'k_myontech_shirt02_emg', # Right Arm
    'k_myontech_shirt03_emg'  # Trunk
]
FATIGUE_ALERT_TOPIC = 'emg_processed_fatigue' # Topic to publish alerts to
CONSUMER_GROUP_ID = 'python-emg-fatigue-processor-group-v2' # Changed group ID slightly for clean start
# Signal Processing Settings
SAMPLING_RATE_HZ = 100.0 # IMPORTANT: Match your sensor's rate!
FFT_WINDOW_SEC = 1.0     # Duration of window for FFT/MDF (e.g., 1 second)
FFT_WINDOW_SAMPLES = int(SAMPLING_RATE_HZ * FFT_WINDOW_SEC)
# Optional: Ensure power of 2 if needed, though scipy.signal.welch handles various lengths
# FFT_WINDOW_SAMPLES = 128 # Example power of 2 close to 100 samples

# Trend Analysis Settings
TREND_WINDOW_SEC = 60 # How far back to look for MDF trend
MIN_MDF_POINTS_FOR_TREND = 10 # Min MDF values needed for regression
MDF_SLOPE_FATIGUE_THRESHOLD = -0.15 # Hz/sec decrease threshold - NEEDS CALIBRATION!
# --- Ensure this definition matches usage below ---
REGRESSION_R_SQUARED_THRESHOLD = 0.3 # Min R^2 for trend significance
# ---
ALERT_COOLDOWN_SEC = 60 # Min seconds between alerts for the same muscle

# Muscles to process (from your Flink job config)
MUSCLES_TO_MONITOR = {
    "trapezius_left", "trapezius_right",
    "deltoids_left", "deltoids_right",
    "wrist_extensors_right", "wrist_flexor_right"
}

# Logging Setup
# Use DEBUG level to see the trace messages we will change
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__) # Get logger for this module

# --- State Management ---
# Key: tuple (thingid, muscle_name)
# Value: deque containing tuples (timestamp_ms, value_uV)
emg_buffers = defaultdict(lambda: deque(maxlen=int(FFT_WINDOW_SAMPLES * 1.5))) # Buffer slightly more than needed

# Key: tuple (thingid, muscle_name)
# Value: deque containing tuples (timestamp_ms, mdf_value)
mdf_history = defaultdict(lambda: deque(maxlen=int(TREND_WINDOW_SEC * (SAMPLING_RATE_HZ/FFT_WINDOW_SAMPLES)*2))) # Estimate max points needed

# Key: tuple (thingid, muscle_name)
# Value: timestamp (seconds since epoch) of the last alert sent
last_alert_time = defaultdict(float)

# --- Helper Functions ---
def parse_flink_timestamp(ts_str):
    """Parses 'yyyy-MM-dd HH:mm:ss' to milliseconds since epoch (UTC)."""
    if not ts_str: return None # Handle empty or null string
    try:
        dt_obj = datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        return int(dt_obj.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse timestamp '{ts_str}': {e}")
        return None

def calculate_mdf(signal, fs, nperseg):
    """Calculates MDF using Welch method for PSD."""
    if len(signal) < nperseg:
        logger.debug(f"Not enough samples for Welch: got {len(signal)}, need {nperseg}")
        return np.nan

    signal_detrended = detrend(signal, type='linear')

    try:
        frequencies, psd = welch(signal_detrended, fs=fs, nperseg=nperseg, scaling='density', window='hann')
    except ValueError as e:
        logger.warning(f"Welch calculation failed: {e}. Signal length: {len(signal_detrended)}, nperseg: {nperseg}")
        return np.nan
    except Exception as e:
        logger.error(f"Unexpected error during Welch calculation: {e}", exc_info=True)
        return np.nan

    total_power = np.sum(psd)
    if total_power < 1e-10:
        logger.debug("Total power near zero, cannot calculate MDF.")
        return np.nan

    cumulative_power = np.cumsum(psd)
    try:
        median_freq_index = np.searchsorted(cumulative_power, total_power / 2.0, side='left')
        if median_freq_index >= len(frequencies):
            median_freq_index = len(frequencies) - 1
        return frequencies[median_freq_index]
    except IndexError:
        logger.warning("Could not find median frequency index (check PSD calculation).")
        return np.nan

# --- Kafka Clients ---
consumer = None # Initialize to None
producer = None # Initialize to None
try:
    consumer = KafkaConsumer(
        *RAW_EMG_TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: v,
        auto_offset_reset='latest',
        consumer_timeout_ms=1000
    )
    logger.info("Kafka Consumer connected.")
except Exception as e:
    logger.error(f"Failed to create Kafka Consumer: {e}")
    exit(1)

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info("Kafka Producer connected.")
except Exception as e:
    logger.error(f"Failed to create Kafka Producer: {e}")
    if consumer: consumer.close()
    exit(1)


# --- Main Processing Loop ---
logger.info("Starting EMG fatigue processing loop...")
try:
    while True:
        message_pack = consumer.poll(timeout_ms=1000)

        if not message_pack:
            time.sleep(0.1)
            continue

        for topic_partition, records in message_pack.items():
            for record in records:
                try:
                    message_value = record.value.decode('utf-8')
                    logger.debug(f"Received raw message from topic {record.topic}: {message_value}")
                    data = json.loads(message_value)

                    thingid = data.get("thingid")
                    timestamp_str = data.get("timestamp")

                    if not thingid or not timestamp_str:
                        logger.warning(f"Skipping record with missing thingid or timestamp: {message_value}")
                        continue

                    timestamp_ms = parse_flink_timestamp(timestamp_str)
                    if timestamp_ms is None:
                        logger.warning(f"Skipping record with unparseable timestamp: {message_value}")
                        continue

                    for muscle_name, value_uV in data.items():
                        if muscle_name in ["thingid", "timestamp"]: continue
                        if muscle_name not in MUSCLES_TO_MONITOR: continue

                        try:
                            value_uV = float(value_uV)
                            if np.isnan(value_uV) or np.isinf(value_uV): continue
                        except (ValueError, TypeError): continue

                        key = (thingid, muscle_name)
                        emg_buffers[key].append((timestamp_ms, value_uV))

                        if len(emg_buffers[key]) >= FFT_WINDOW_SAMPLES:
                            signal_window = np.array([val for ts, val in list(emg_buffers[key])[-FFT_WINDOW_SAMPLES:]])
                            mdf = calculate_mdf(signal_window, SAMPLING_RATE_HZ, FFT_WINDOW_SAMPLES)

                            if not np.isnan(mdf):
                                logger.debug(f"MDF Calculated: Key={key}, Time={timestamp_ms}, MDF={mdf:.2f}")
                                mdf_history[key].append((timestamp_ms, mdf))

                                cutoff_ts = timestamp_ms - (TREND_WINDOW_SEC * 1000)
                                while mdf_history[key] and mdf_history[key][0][0] < cutoff_ts:
                                    mdf_history[key].popleft()

                                current_time_sec = time.time()
                                if current_time_sec < last_alert_time[key] + ALERT_COOLDOWN_SEC:
                                    logger.debug(f"Key {key}: In alert cooldown period.")
                                    continue

                                if len(mdf_history[key]) >= MIN_MDF_POINTS_FOR_TREND:
                                    history_points = list(mdf_history[key])
                                    first_ts = history_points[0][0]
                                    times = np.array([(ts - first_ts) / 1000.0 for ts, mdf_val in history_points])
                                    mdfs = np.array([mdf_val for ts, mdf_val in history_points])

                                    if len(times) < 2: continue

                                    try:
                                        slope, intercept, r_value, p_value, std_err = linregress(times, mdfs)
                                        if np.isnan(slope) or np.isnan(r_value):
                                             logger.warning(f"Linear regression resulted in NaN for key {key}. Slope={slope}, R={r_value}")
                                             continue
                                        r_squared = r_value**2

                                        logger.debug(f"Key: {key}, MDF Trend Check: Slope={slope:.4f} Hz/s, R^2={r_squared:.3f}, N={len(times)}")

                                        # --- Ensure this usage matches the definition ---
                                        if slope < MDF_SLOPE_FATIGUE_THRESHOLD and r_squared >= REGRESSION_R_SQUARED_THRESHOLD:
                                        # ---
                                            logger.warning(f"FATIGUE ALERT (MDF Trend): Key={key} | Slope: {slope:.3f}, R^2: {r_squared:.3f}")
                                            alert_payload = {
                                                "thingId": thingid, "timestamp": timestamp_ms,
                                                "feedbackType": "emgFatigueAlert", "muscle": muscle_name,
                                                "severity": "HIGH", "reason": "Decreasing MDF Trend Detected",
                                                "mdfSlopeHzPerSec": round(slope, 3), "regressionRSquared": round(r_squared, 3),
                                                "trendWindowSeconds": TREND_WINDOW_SEC, "slopeThreshold": MDF_SLOPE_FATIGUE_THRESHOLD
                                            }
                                            producer.send(FATIGUE_ALERT_TOPIC, value=alert_payload)
                                            last_alert_time[key] = current_time_sec
                                            # mdf_history[key].clear() # Optional: Clear history after alert?

                                    except ValueError as ve:
                                         logger.warning(f"Linear regression failed for key {key}: {ve}")

                except json.JSONDecodeError:
                    logger.warning(f"Could not decode JSON: {record.value.decode('utf-8', errors='ignore')}")
                except Exception as e:
                    logger.error(f"Error processing record: {record.value.decode('utf-8', errors='ignore')} | Error: {e}", exc_info=True)

except KeyboardInterrupt:
    logger.info("Shutdown signal received.")
finally:
    logger.info("Closing Kafka Consumer and Producer.")
    if consumer: consumer.close()
    if producer:
        try: producer.flush(timeout=10)
        except Exception as e: logger.error(f"Error flushing Kafka producer: {e}")
        finally: producer.close(timeout=10)
    logger.info("Clients closed. Exiting.")

# --- End Main Loop ---
