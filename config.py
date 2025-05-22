# config.py

# --- Kafka Configuration ---
KAFKA_BROKERS = ['192.168.50.105:9092', '192.168.50.105:9093', '192.168.50.105:9094']
EMG_MDF_INPUT_TOPIC = "th_emg_for_mdf_processing"
FATIGUE_ALERT_TOPIC = 'th_emg_mdf_fatigue_alerts'
MDF_VISUALIZATION_TOPIC = 'th_emg_mdf_visualization_data'
CONSUMER_GROUP_ID = 'python-emg-mdf-processor-v5' # Incremented version for new structure

# --- TimescaleDB Configuration ---
DB_HOST = "192.168.50.208"
DB_PORT = "5432"
DB_NAME = "th_emg_processed_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
MDF_TABLE_NAME = "th_emg_mdf_values"

# --- EMG Processing Parameters ---
SAMPLING_RATE_HZ = 100.0  # Hz
FFT_WINDOW_SEC = 1.0      # Duration of window for FFT/MDF
FFT_OVERLAP_PERCENT = 0.5 # 50% overlap
WINDOW_TYPE_WELCH = 'hann'
# NPERSEG_WELCH and NOVERLAP_WELCH will be derived in the main script

# Optional Bandpass Pre-filtering settings
FILTER_ORDER = 4
LOWCUT_HZ = 20.0
HIGHCUT_HZ = 40.0
APPLY_PRE_FILTERING = False # Set to True to enable

# --- Fatigue Analysis Parameters ---
TREND_WINDOW_SEC = 60  # Seconds for MDF trend analysis
MIN_MDF_POINTS_FOR_TREND = 10 # Minimum MDF values in the trend window to calculate slope
MDF_SLOPE_FATIGUE_THRESHOLD = -0.10 # Hz/second
REGRESSION_R_SQUARED_THRESHOLD = 0.25 # Minimum R-squared value for a valid trend
ALERT_COOLDOWN_SEC = 60 # Seconds before another alert for the same muscle

# --- Muscles to Monitor ---
# Ensure these names exactly match the 'muscle' field in your Kafka messages
MUSCLES_TO_MONITOR_FOR_MDF = {
    "trapezius_right",
    "deltoids_right",
    "latissimus_right"
}

# --- Threading Configuration ---
FATIGUE_ANALYSIS_WORKERS = 3 # Number of worker threads for fatigue analysis

# --- Logging Configuration ---
LOG_LEVEL = "INFO" # e.g., DEBUG, INFO, WARNING, ERROR
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(name)s - %(module)s.%(funcName)s - %(message)s'