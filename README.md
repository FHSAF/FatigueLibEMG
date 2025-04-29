# Real-time EMG Fatigue Processor

This Python script analyzes Electromyography (EMG) data streamed via Kafka to detect potential muscle fatigue in real-time based on frequency analysis (Median Frequency - MDF).

## Functionality

1.  **Consume Raw EMG Data:** The script connects to an Apache Kafka cluster and subscribes to specified topics containing raw EMG data (likely in microvolts, µV) published from Myontech EMG shirts (or similar sensors). It expects data in JSON format with fields like `thingid`, `timestamp`, and muscle activation values (e.g., `deltoids_right`, `trapezius_left`).
2.  **Calculate Median Frequency (MDF):** For each specified muscle being monitored:
    * It buffers the incoming raw EMG samples over time.
    * Once enough samples are collected for a defined window (e.g., 1 second worth of data), it performs signal processing:
        * **Detrending:** Removes the mean value from the signal window.
        * **Windowing:** Applies a Hanning window to reduce spectral leakage.
        * **FFT & PSD:** Calculates the Power Spectral Density using the Welch method (`scipy.signal.welch`).
        * **MDF Calculation:** Determines the Median Frequency (the frequency that divides the power spectrum in half).
    * It outputs the calculated MDF value along with a timestamp.
3.  **Detect Fatigue Trend:**
    * It maintains a history of recent MDF values for each muscle over a longer analysis window (e.g., 60 seconds).
    * It performs linear regression on this MDF history to calculate the slope (trend) over time.
    * It checks if the slope shows a statistically significant decreasing trend (i.e., slope is negative and below a defined threshold, and the R-squared value meets a quality threshold).
4.  **Publish Fatigue Alerts:**
    * If a significant decreasing MDF trend (indicating fatigue) is detected for a muscle:
        * It constructs a JSON alert message containing details like the muscle name, severity ("HIGH"), the calculated slope, R-squared value, and timestamps.
        * It publishes this alert message to a dedicated Kafka topic (e.g., `emg_processed_fatigue`).
    * A cooldown period prevents repeated alerts for the same muscle immediately after one is sent.

## Prerequisites

* Python 3 installed.
* Required Python libraries installed (preferably in a virtual environment):
    ```bash
    pip install kafka-python numpy scipy
    ```
* Access to a running Apache Kafka cluster.
* Kafka topics created for raw EMG input and fatigue alert output.

## Configuration

Before running, configure the constants at the top of the `emg_fatigue_processor.py` script:

* `KAFKA_BROKERS`: List of your Kafka broker addresses (e.g., `['192.168.1.100:9092']`).
* `RAW_EMG_TOPICS`: List of Kafka topics containing the raw EMG data (e.g., `['k_myontech_shirt01_emg', 'k_myontech_shirt02_emg', 'k_myontech_shirt03_emg']`).
* `FATIGUE_ALERT_TOPIC`: The Kafka topic where fatigue alerts will be published (e.g., `'emg_processed_fatigue'`).
* `CONSUMER_GROUP_ID`: A unique ID for the Kafka consumer group (e.g., `'python-emg-fatigue-processor-group'`).
* `SAMPLING_RATE_HZ`: **Crucial:** Set this to the exact sampling frequency of your EMG sensors (e.g., `100.0` for 100 Hz).
* `FFT_WINDOW_SAMPLES`: Number of samples for MDF calculation (derived from `FFT_WINDOW_SEC` and `SAMPLING_RATE_HZ`). Consider powers of 2 if using FFT libraries that require it.
* `TREND_WINDOW_SEC`: Duration (seconds) to analyze MDF trend (e.g., `60`).
* `MIN_MDF_POINTS_FOR_TREND`: Minimum MDF values needed for regression (e.g., `10`).
* `MDF_SLOPE_FATIGUE_THRESHOLD`: Negative slope (Hz/sec) indicating fatigue (e.g., `-0.15`). **Requires calibration.**
* `REGRESSION_R_SQUARED_THRESHOLD`: Minimum R-squared value for trend significance (e.g., `0.3`).
* `ALERT_COOLDOWN_SEC`: Minimum time between alerts for the same muscle (e.g., `60`).
* `MUSCLES_TO_MONITOR`: A Python set containing the exact string names of the muscles (matching JSON keys) to analyze (e.g., `{"trapezius_left", "trapezius_right", ...}`).

## Running the Processor

### Method 1: Directly in Terminal (for testing)

1.  **Activate Virtual Environment:**
    ```bash
    source /path/to/your/venv/bin/activate
    ```
    (Use `venv\Scripts\activate.bat` or `venv\Scripts\Activate.ps1` on Windows)
2.  **Navigate to Script Directory:**
    ```bash
    cd /path/to/your/script/directory
    ```
3.  **Run Script:**
    ```bash
    python emg_fatigue_processor.py
    ```
4.  Press `Ctrl+C` to stop the script.

### Method 2: As a Systemd Service (Recommended for long-running)

This method runs the script as a background service managed by the system on Linux (like your Ubuntu VM).

1.  **Create Service File:** Create a file named `emg-fatigue-processor.service` in `/etc/systemd/system/` using `sudo nano` or another editor.
2.  **Paste Configuration:** Paste the following content into this file:
    ```ini
    [Unit]
    Description=EMG Fatigue Processor Service (Python Script)
    After=network.target kafka.service # Optional: Add kafka service dependency if managed by systemd

    [Service]
    # !!! CHANGE User, Group, WorkingDirectory, and ExecStart paths !!!
    User=your_vm_user
    Group=your_vm_group
    WorkingDirectory=/path/to/your/script/directory
    ExecStart=/path/to/your/venv/bin/python /path/to/your/script/directory/emg_fatigue_processor.py

    Restart=on-failure
    RestartSec=5

    StandardOutput=journal
    StandardError=journal

    [Install]
    WantedBy=multi-user.target
    ```
3.  **Customize:** Edit the `User`, `Group`, `WorkingDirectory`, and `ExecStart` lines in the file to match your specific setup (correct username, paths to your virtual environment's Python, and script location).
4.  **Save and Close.**
5.  **Reload Systemd:**
    ```bash
    sudo systemctl daemon-reload
    ```
6.  **Enable Service (Start on Boot):**
    ```bash
    sudo systemctl enable emg-fatigue-processor.service
    ```
7.  **Manage the Service:** Use the following `systemctl` commands:
    * **Start:** `sudo systemctl start emg-fatigue-processor.service`
    * **Stop:** `sudo systemctl stop emg-fatigue-processor.service`
    * **Restart:** `sudo systemctl restart emg-fatigue-processor.service`
    * **Check Status:** `sudo systemctl status emg-fatigue-processor.service`
    * **View Logs (Recent):** `sudo journalctl -u emg-fatigue-processor.service -n 50 --no-pager`
    * **Follow Logs (Live):** `sudo journalctl -f -u emg-fatigue-processor.service` (Press Ctrl+C to exit)
    * **Disable Auto-Start:** `sudo systemctl disable emg-fatigue-processor.service`

This README provides a basic guide to understanding and running the Python EMG fatigue processor script. Remember to **calibrate the fatigue detection thresholds** for accurate results in your specific application.
