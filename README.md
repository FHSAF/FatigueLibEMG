# Real-time EMG Muscle Fatigue Detection using Median Frequency (MDF) Analysis

This Python script consumes Electromyography (EMG) data for specific muscles from an Apache Kafka topic, calculates the Median Frequency (MDF) of the EMG signal's power spectrum, analyzes the trend of these MDF values over time to detect muscle fatigue, and publishes fatigue alerts to another Kafka topic. Additionally, it publishes each calculated MDF value to a dedicated Kafka topic for real-time visualization.

The script is designed to integrate with a larger system where EMG data might be pre-processed or filtered by an upstream application (e.g., Apache Flink) before being sent to this script.

## Features

-   **Real-time Processing:** Consumes EMG data streams from Kafka.
-   **MDF Calculation:**
    -   Buffers incoming raw EMG samples for specified window.
    -   Optionally applies a Butterworth bandpass filter.
    -   Detrends the signal segment.
    -   Calculates Power Spectral Density (PSD) using **Welch's method** (`scipy.signal.welch`).
    -   Computes Median Frequency (MDF) from the PSD.
-   **Fatigue Trend Analysis:**
    -   Maintains a history of MDF values for each muscle over a defined trend window.
    -   Performs linear regression on the MDF history to find the slope and R-squared value.
    -   Generates a fatigue alert if the MDF slope shows a significant decrease below a configured threshold and the R-squared value indicates a reliable trend.
-   **Alerting & Visualization:**
    -   Publishes detailed fatigue alert messages (JSON format) to a dedicated Kafka topic. Alerts include `alertTriggerTimestamp` and calculated delay metrics.
    -   Publishes each calculated MDF value (JSON format) with its corresponding source timestamp to a separate Kafka topic for real-time visualization.
-   **Configurable Parameters:** Most signal processing, trend analysis, and Kafka parameters are configurable at the top of the script.
-   **State Management:** Manages data buffers and MDF history independently for each muscle and `thingid`.
-   **Cooldown Mechanism:** Prevents alert spamming for the same muscle.
-   **Robust Kafka Integration:** Includes retry logic for Kafka client initialization.

## How it Works (Methodology)

1.  **Data Ingestion:**
    * The script connects to specified Kafka brokers and subscribes to `EMG_MDF_INPUT_TOPIC`.
    * It expects JSON messages from Flink, each containing data for a single muscle reading:
        ```json
        {
          "thingid": "device_identifier",
          "sourceTimestampMs": 1678886400123, // Original sensor event timestamp (epoch ms)
          "flinkSentTimestampMs": 1678886400200, // Timestamp when Flink sent this data (epoch ms)
          "muscle": "muscle_name_string",     // e.g., "deltoids_right"
          "value": 123.45                     // Raw EMG value in microvolts
        }
        ```

2.  **EMG Sample Buffering:**
    * For each `(thingid, muscle_name)` combination, raw EMG samples (`value`) along with their `sourceTimestampMs` and `flinkSentTimestampMs` are collected in a fixed-size buffer (`emg_sample_buffer`).
    * The buffer size is determined by `NPERSEG_WELCH` (typically equal to `FFT_WINDOW_SAMPLES`).

3.  **MDF Calculation (per muscle, per window):**
    * When the buffer for a muscle is full:
        a.  The collected EMG samples are extracted.
        b.  **(Optional Pre-filtering):** If `APPLY_PRE_FILTERING` is true, a Butterworth bandpass filter (`butter_bandpass_filter` function using `scipy.signal.butter` and `scipy.signal.filtfilt`) is applied to remove noise outside the specified frequency band (e.g., 20-40Hz, though typical EMG is 20-450Hz).
        c.  **Detrending:** A linear trend is removed from the signal segment using `scipy.signal.detrend`. This is important because non-stationarities or baseline shifts can distort the power spectrum.
        d.  **PSD Estimation:** The Power Spectral Density (PSD) is estimated from the detrended signal segment using **Welch's method** (`scipy.signal.welch`). This method divides the segment into smaller (potentially overlapping) sub-segments, applies a window function (e.g., 'Hann') to each, computes the FFT-based periodogram for each, and then averages these periodograms. This reduces the variance of the PSD estimate.
            * `NPERSEG_WELCH`: Length of each segment.
            * `NOVERLAP_WELCH`: Number of overlapping samples between segments.
            * `WINDOW_TYPE_WELCH`: Window function applied (e.g., 'hann').
        e.  **MDF from PSD:** The Median Frequency (MDF) is calculated from the resulting PSD (`calculate_mdf_from_psd` function). MDF is the frequency that divides the total power of the spectrum into two equal halves.
    * The timestamp associated with this calculated MDF value (`mdf_calc_timestamp_ms`) is the `sourceTimestampMs` of the *last raw EMG sample* that completed the FFT window.
    * Average `sourceTimestampMs` and average `flinkSentTimestampMs` for the samples within the current FFT window are also calculated.

4.  **MDF Data Publishing for Visualization:**
    * Each successfully calculated MDF value, along with `thingid`, `muscle_name`, its `mdf_calc_timestamp_ms`, and the average timestamps for its window data, is immediately formatted into a JSON payload.
    * This payload is published to the `MDF_VISUALIZATION_TOPIC`.
        ```json
        {
            "thingId": "device_identifier",
            "muscle": "muscle_name",
            "sourceTimestampMs": 1678886400123, // MDF point's representative source time
            "mdfValue": 55.75,
            "avgFlinkSentTimestampForWindowDataMs": 1678886400200, // Avg Flink send time for data in this MDF's FFT window
            "avgSourceTimestampForWindowDataMs": 1678886400070  // Avg source time for data in this MDF's FFT window
        }
        ```

5.  **MDF Trend Analysis & Fatigue Detection:**
    * The calculated `(mdf_calc_timestamp_ms, mdf_value, avg_flink_sent_ts_for_this_fft_window, avg_source_ts_for_this_fft_window)` tuple is stored in a fixed-size history deque (`mdf_history`) for each muscle.
    * Old MDF values falling outside the `TREND_WINDOW_SEC` are pruned from the history.
    * If the number of MDF points in the history meets `MIN_MDF_POINTS_FOR_TREND`:
        a.  **Linear Regression:** `scipy.stats.linregress` is used to calculate the slope and R-squared value of the MDF values over their relative times in the history.
        b.  **Fatigue Check:** If the `slope` is more negative than `MDF_SLOPE_FATIGUE_THRESHOLD` AND `r_squared` is greater than or equal to `REGRESSION_R_SQUARED_THRESHOLD`, a fatigue condition is identified.
    * An `ALERT_COOLDOWN_SEC` is applied to prevent sending alerts too frequently for the same muscle.

6.  **Alert Publishing with Delay Metrics:**
    * If fatigue is detected, a JSON `alert_payload` is constructed.
    * **`alertTriggerTimestamp`**: This is the `mdf_calc_timestamp_ms` (original source timestamp) of the *last MDF data point* that confirmed the fatigue trend. This is crucial for Unity to calculate its "Alert Latency."
    * **Delay Metrics Calculated in Python:**
        * `averageMdfDataAgeInTrendMs`: The average age of the *original sensor data* (that formed the MDF values in the current trend window), relative to the `alertTriggerTimestamp`. Calculated as: `alertTriggerTimestamp - average(avg_source_ts_for_fft_window values for MDFs in trend)`.
        * `estimatedFlinkPipelineDelayMs`: An estimate of the average time data took to pass through Flink before being sent to this Python script, for the data points contributing to the current trend. Calculated as: `average(avg_flink_sent_ts_for_fft_window values for MDFs in trend) - average(avg_source_ts_for_fft_window values for MDFs in trend)`.
        * `pythonProcessingTimeForAlertMs`: The internal processing latency within this Python script, from the `alertTriggerTimestamp` (an original sensor data time) to when Python is about to send the alert. Calculated as: `python_alert_emission_system_time_ms - alertTriggerTimestamp`.
        * `pythonAlertSentTimestampMs`: The system timestamp (epoch ms) when Python sends the alert.
    * The alert payload (including these delay metrics, muscle info, slope, R², etc.) is published to `FATIGUE_ALERT_TOPIC`.

## Scientific Basis

The fatigue detection methodology implemented in this script is grounded in established principles of sEMG signal processing and muscle physiology:

1.  **MDF as a Primary Fatigue Indicator:**
    * The core concept is that localized muscle fatigue leads to a decrease in the firing rate of motor units and a slowing of muscle fiber conduction velocity (MFCV). These physiological changes cause a compression of the EMG signal's power spectrum towards lower frequencies, resulting in a quantifiable **decrease in the Median Frequency (MDF)**.
    * This relationship is widely documented and validated in biomechanics and ergonomics research.
    * *Relevant scientific resources (from your previously provided list and common knowledge):* (Cifrek et al., 2009, "Surface EMG based muscle fatigue evaluation in biomechanics"), (Hollman et al., 2013, "Does the fast Fourier transformation window length affect the slope..."), (Ma'as et al., 2017, "Real-time muscle fatigue monitoring based on median frequency...").

2.  **Power Spectral Density (PSD) Estimation - Welch's Method:**
    * To calculate MDF, the PSD of the EMG signal is required.
    * The script uses **Welch's method** (`scipy.signal.welch`). This is a standard and robust technique for estimating the PSD of a signal. It works by:
        1.  Dividing the signal segment into smaller, potentially overlapping sub-segments.
        2.  Applying a window function (e.g., Hann window, specified by `WINDOW_TYPE_WELCH`) to each sub-segment to reduce spectral leakage.
        3.  Calculating the periodogram (FFT-based power spectrum) for each windowed sub-segment.
        4.  Averaging these periodograms to obtain a smoother, less noisy PSD estimate.
    * *Relevant scientific resources:* (Corvini & Conforto, 2022, "A Simulation Study to Assess the Factors of Influence on Mean and Median Frequency...") compare Welch's method favorably with others for EMG fatigue analysis. The general principles of PSD estimation via FFT are foundational in signal processing.

3.  **Signal Pre-processing:**
    * **Detrending (`scipy.signal.detrend`):** Removes linear trends from the EMG segment before PSD calculation. This is important because baseline shifts or slow drifts in the signal can distort the low-frequency components of the spectrum and affect MDF.
    * **(Optional) Bandpass Filtering (`butter_bandpass_filter`):** While currently disabled (`APPLY_PRE_FILTERING = False`), bandpass filtering (e.g., 20-450Hz for general sEMG, or a narrower band if focusing only on frequencies relevant to MDF changes) can remove noise outside the primary EMG signal band. The use of `filtfilt` ensures zero-phase distortion.

4.  **Trend Analysis using Linear Regression:**
    * Simply observing a single MDF value is usually insufficient for reliable fatigue detection due to inherent variability. Analyzing the **trend of MDF values over time** is a more robust approach.
    * A consistent **negative slope** in the MDF trend indicates the progression of fatigue.
    * The script uses **linear regression** (`scipy.stats.linregress`) to calculate this slope from a history of MDF values.
    * The **R-squared (R²) value** from the regression quantifies how well the linear model fits the MDF data points, serving as an indicator of the trend's reliability or significance.
    * *Relevant scientific resources:* The approach of tracking MDF slope is discussed in (Hollman et al., 2013) and (Ma'as et al., 2017).

**Parameters for Calibration:**
The accuracy and sensitivity of the fatigue detection depend heavily on the correct calibration of parameters like `FFT_WINDOW_SEC`, `TREND_WINDOW_SEC`, `MDF_SLOPE_FATIGUE_THRESHOLD`, and `REGRESSION_R_SQUARED_THRESHOLD` for the specific muscles and tasks being monitored.

## Setup and Usage

1.  **Prerequisites:**
    * Python 3.x
    * Required Python packages: `numpy`, `scipy`, `kafka-python` (install via `pip install numpy scipy kafka-python`).
2.  **Configuration:**
    * Modify the constants at the top of the script:
        * `KAFKA_BROKERS`: Set to your Kafka broker addresses.
        * `EMG_MDF_INPUT_TOPIC`: Ensure this matches the Kafka topic where your Flink job publishes the filtered EMG data for the target muscles. Expected JSON format per message from Flink:
            ```json
            {
              "thingid": "some_id",
              "sourceTimestampMs": 1678886400123,
              "flinkSentTimestampMs": 1678886400200,
              "muscle": "deltoids_right",
              "value": 45.67
            }
            ```
        * `FATIGUE_ALERT_TOPIC`: Topic for fatigue alerts.
        * `MDF_VISUALIZATION_TOPIC`: Topic for individual MDF values.
        * `SAMPLING_RATE_HZ`: **Critically important.** Must match your EMG sensor's actual sampling rate.
        * Adjust `FFT_WINDOW_SEC`, `TREND_WINDOW_SEC`, and fatigue thresholds (`MDF_SLOPE_FATIGUE_THRESHOLD`, `REGRESSION_R_SQUARED_THRESHOLD`) based on experimentation and calibration for your specific application.
        * Verify `MUSCLES_TO_MONITOR_FOR_MDF` contains the exact muscle names as they appear in the `muscle` field of the JSON messages from Flink.
3.  **Running the Script:**
    ```bash
    python your_script_name.py
    ```
    The script will connect to Kafka and start processing messages.

## Output

1.  **Fatigue Alerts (to `FATIGUE_ALERT_TOPIC`):**
    When fatigue is detected, a JSON message is published, including:
    * `thingId`, `muscle`, `severity`, `reason`
    * `alertTriggerTimestamp`: The original source timestamp (epoch ms) of the data point that confirmed the fatigue trend.
    * `mdfSlopeHzPerSec`, `regressionRSquared`
    * `averageMdfDataAgeInTrendMs`: Average age of the source data (contributing to MDFs in the trend) relative to the `alertTriggerTimestamp`.
    * `estimatedFlinkPipelineDelayMs`: Estimated average delay for data passing through Flink before reaching this script, for data in the trend.
    * `pythonProcessingTimeForAlertMs`: Python's internal processing time from `alertTriggerTimestamp` to alert emission.
    * `pythonAlertSentTimestampMs`: System time (epoch ms) when Python sent the alert.
    * Other trend details.

2.  **MDF Visualization Data (to `MDF_VISUALIZATION_TOPIC`):**
    For each successfully calculated MDF value:
    ```json
    {
        "thingId": "device_identifier",
        "muscle": "muscle_name",
        "sourceTimestampMs": 1678886400123, // Source timestamp of this MDF point
        "mdfValue": 55.75,
        "avgFlinkSentTimestampForWindowDataMs": 1678886400200, 
        "avgSourceTimestampForWindowDataMs": 1678886400070
    }
    ```

This script provides a robust, scientifically-backed method for real-time MDF-based muscle fatigue assessment with detailed logging and outputs for both alerts and continuous monitoring.