# fatigue_analyzer.py
import time
import numpy as np
from scipy.stats import linregress
from kafka import errors as kafka_errors
import logging
import json
import statistics

# Import necessary constants from config
from config import (
    MIN_MDF_POINTS_FOR_TREND,
    MDF_SLOPE_FATIGUE_THRESHOLD,
    REGRESSION_R_SQUARED_THRESHOLD,
    ALERT_COOLDOWN_SEC,
    FATIGUE_ALERT_TOPIC
)

logger = logging.getLogger(__name__)

def perform_fatigue_analysis_and_alert(
    key_tuple,
    mdf_history_data_copy,
    current_mdf_trigger_timestamp_ms,
    kafka_producer_ref,
    last_alert_dict_ref,
    lock_ref
):
    """
    Performs fatigue trend analysis and sends an alert if criteria are met.
    This function is designed to be run in a worker thread.
    """
    thingid, muscle_name = key_tuple
    logger.debug(f"Worker {key_tuple}: Starting fatigue analysis with {len(mdf_history_data_copy)} points.")

    try:
        if len(mdf_history_data_copy) < MIN_MDF_POINTS_FOR_TREND:
            logger.debug(f"Worker {key_tuple}: Not enough data points ({len(mdf_history_data_copy)}) for trend. Min: {MIN_MDF_POINTS_FOR_TREND}")
            return

        # Extract timestamps and MDF values for regression
        # mdf_history_data_copy elements are (mdf_calc_source_timestamp_ms, mdf, avg_flink_sent_ts_for_fft_window, avg_source_ts_in_current_fft_window)
        first_mdf_source_ts_in_trend = mdf_history_data_copy[0][0]
        times_rel_sec_trend = np.array([(item[0] - first_mdf_source_ts_in_trend) / 1000.0 for item in mdf_history_data_copy])
        mdfs_values_trend = np.array([item[1] for item in mdf_history_data_copy])

        if len(times_rel_sec_trend) < 2: # Need at least 2 points for a line
            logger.debug(f"Worker {key_tuple}: Not enough unique time points for linregress after processing history copy.")
            return

        slope, intercept, r_value, p_value, std_err = linregress(times_rel_sec_trend, mdfs_values_trend)

        if np.isnan(slope) or np.isnan(r_value):
            logger.warning(f"Worker {key_tuple}: Linreg resulted in NaN. Slope={slope}, R-value={r_value}")
            return
        r_squared = r_value**2

        logger.info(f"Worker {key_tuple}: MDF Trend Calculated. Slope={slope:.4f} Hz/s, R^2={r_squared:.3f}, Points={len(mdfs_values_trend)}")

        if slope < MDF_SLOPE_FATIGUE_THRESHOLD and r_squared >= REGRESSION_R_SQUARED_THRESHOLD:
            worker_current_time_s = time.time()

            with lock_ref:
                if worker_current_time_s < last_alert_dict_ref.get(key_tuple, 0) + ALERT_COOLDOWN_SEC:
                    logger.debug(f"Worker {key_tuple}: Alert cooldown active. Last alert: {last_alert_dict_ref.get(key_tuple, 0):.2f}, Current: {worker_current_time_s:.2f}")
                    return
                last_alert_dict_ref[key_tuple] = worker_current_time_s # Update last alert time

            # Prepare and send alert
            python_alert_emission_time_ms = int(worker_current_time_s * 1000)

            source_timestamps_of_mdf_data_in_trend = [item[3] for item in mdf_history_data_copy] # avg_source_ts_for_fft_window
            flink_sent_timestamps_of_mdf_data_in_trend = [item[2] for item in mdf_history_data_copy] # avg_flink_sent_ts_for_fft_window

            avg_source_ts_of_data_in_trend_overall = statistics.mean(source_timestamps_of_mdf_data_in_trend) if source_timestamps_of_mdf_data_in_trend else current_mdf_trigger_timestamp_ms
            avg_mdf_data_age_in_trend_ms_py = int(current_mdf_trigger_timestamp_ms - avg_source_ts_of_data_in_trend_overall)

            avg_flink_sent_ts_for_trend_mds_overall = statistics.mean(flink_sent_timestamps_of_mdf_data_in_trend) if flink_sent_timestamps_of_mdf_data_in_trend else 0
            flink_pipeline_delay_for_trend_data_ms = 0
            if avg_flink_sent_ts_for_trend_mds_overall > 0 and avg_source_ts_of_data_in_trend_overall > 0:
                flink_pipeline_delay_for_trend_data_ms = int(avg_flink_sent_ts_for_trend_mds_overall - avg_source_ts_of_data_in_trend_overall)

            python_internal_processing_delay_ms = python_alert_emission_time_ms - current_mdf_trigger_timestamp_ms

            alert_payload = {
                "thingId": thingid,
                "alertTriggerTimestamp": current_mdf_trigger_timestamp_ms,
                "feedbackType": "emgMdfFatigueAlert",
                "muscle": muscle_name,
                "severity": "HIGH",
                "reason": "Decreasing MDF Trend Detected (Python Worker)",
                "mdfSlopeHzPerSec": round(slope, 4),
                "regressionRSquared": round(r_squared, 3),
                "trendWindowSecondsUsed": int(round((mdf_history_data_copy[-1][0] - first_mdf_source_ts_in_trend) / 1000.0)),
                "numberOfMdfPointsInTrend": len(mdf_history_data_copy),
                "mdfSlopeThreshold": MDF_SLOPE_FATIGUE_THRESHOLD,
                "rSquaredThreshold": REGRESSION_R_SQUARED_THRESHOLD,
                "lastMdfValueInTrend": round(mdfs_values_trend[-1], 2),
                "averageMdfDataAgeInTrendMs": avg_mdf_data_age_in_trend_ms_py,
                "estimatedFlinkPipelineDelayMs": flink_pipeline_delay_for_trend_data_ms,
                "pythonProcessingTimeForAlertMs": python_internal_processing_delay_ms,
                "pythonAlertSentTimestampMs": python_alert_emission_time_ms
            }
            kafka_producer_ref.send(FATIGUE_ALERT_TOPIC, value=alert_payload)
            kafka_producer_ref.flush(timeout=20) # Timeout for flushing in worker
            logger.warning(f"WORKER {key_tuple}: MDF FATIGUE ALERT SENT | Alert: {json.dumps(alert_payload)}")

    except kafka_errors.KafkaTimeoutError:
        logger.error(f"Worker {key_tuple}: Timeout flushing alert message to Kafka.")
    except ValueError as ve: # Catches issues from linregress if data is unsuitable (e.g. not enough points after all)
        logger.warning(f"Worker {key_tuple}: Linreg failed for key {key_tuple}: {ve}.")
    except Exception as ex_trend_worker:
        logger.error(f"Worker {key_tuple}: Unexpected error in trend analysis: {ex_trend_worker}", exc_info=True)