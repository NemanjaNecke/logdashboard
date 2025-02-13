# services/analyze/iis_analyze.py

import os
from datetime import datetime
import pandas as pd
from collections import defaultdict
import logging
import numpy as np
import xlsxwriter  # Ensure you have xlsxwriter installed

class IISLogAnalyzer:
    """
    Class to analyze IIS logs and export reports to Excel.
    Supports single file, cluster mode (merging two files), and multiple files from the same folder.
    Includes class-based logging for detailed tracing.
    Now also accepts extra_params for custom thresholds, columns, etc.
    """

    def __init__(self, logger=None):
        """
        Initializes the IISLogAnalyzer with an optional logger.
        """
        if logger is None:
            self.logger = logging.getLogger(self.__class__.__name__)  # pylint: disable=no-member
        else:
            self.logger = logger

    def parse_datetime(self, row):
        """
        Combine 'date' and 'time' columns into a datetime object if possible.
        """
        try:
            return datetime.strptime(f"{row['date']} {row['time']}", '%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            self.logger.warning(f"Failed to parse datetime for row: {row}")
            return None

    def load_log_file_in_chunks(
        self,
        log_file_path,
        chunksize=100000,
        interruption_flag=None,
        progress_callback=None,
        selected_columns=None
    ):
        """
        Loads an IIS log file in chunks into pandas DataFrames.

        Args:
            log_file_path (str): Path to the log file.
            chunksize (int): Number of rows per chunk.
            interruption_flag (callable, optional): Function that returns True if interruption is requested.
            progress_callback (callable, optional): Function to report progress messages.
            selected_columns (list of str, optional): If provided, we keep only these columns after reading each chunk.

        Yields:
            pd.DataFrame: DataFrame chunk.
        """
        self.logger.debug(f"Loading log file in chunks: {log_file_path}")
        columns_line = []
        bytes_columns_present = True  # Flag to determine if 'sc-bytes' and 'cs-bytes' are present
        chunk_number = 0  # To track progress

        # Try to find the "#Fields:" line to get column names
        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if line.lower().startswith('#fields:'):
                        fields_str = line.strip().split(':', 1)[1].strip()
                        columns_line = fields_str.split()
                        self.logger.debug(f"Found columns: {columns_line}")
                        break
        except Exception as e:
            self.logger.error(f"Error reading {log_file_path}: {e}")
            if progress_callback:
                progress_callback(f"Error reading {log_file_path}: {e}")
            return

        if not columns_line:
            self.logger.warning(f"No #Fields: line found in {log_file_path}. Skipping.")
            if progress_callback:
                progress_callback(f"No #Fields: line found in {log_file_path}. Skipping.")
            return

        # Now read the file in chunks using those columns
        try:
            for chunk in pd.read_csv(
                log_file_path,
                sep=' ',
                names=columns_line,
                comment='#',       # Ignore all lines that start with '#' after the fields
                header=None,
                engine='python',
                encoding='utf-8',
                chunksize=chunksize,
                on_bad_lines='skip'
            ):
                chunk_number += 1

                # Check for interruption before processing the chunk
                if interruption_flag and interruption_flag():
                    self.logger.info("Interruption detected. Stopping chunk processing.")
                    if progress_callback:
                        progress_callback("Analysis interrupted by the user.")
                    return  # Exit the generator

                # Convert numeric columns if present
                if bytes_columns_present:
                    numeric_cols = ['sc-status', 'time-taken', 'sc-bytes', 'cs-bytes']
                else:
                    numeric_cols = ['sc-status', 'time-taken']  # Exclude bytes columns if not present

                for col in numeric_cols:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                        self.logger.debug(f"Converted column '{col}' to numeric in chunk {chunk_number}.")
                    else:
                        if col in ['sc-bytes', 'cs-bytes']:
                            bytes_columns_present = False
                            self.logger.warning(
                                f"Column '{col}' not found in {log_file_path} chunk {chunk_number}. "
                                "Skipping byte columns for future chunks."
                            )
                            if progress_callback:
                                progress_callback(f"Column '{col}' not found. Skipping related analyses.")
                        else:
                            self.logger.warning(f"Column '{col}' not found in {log_file_path} chunk {chunk_number}.")
                            if progress_callback:
                                progress_callback(f"Column '{col}' not found in chunk {chunk_number}.")

                # Create combined datetime
                if 'date' in chunk.columns and 'time' in chunk.columns:
                    chunk['datetime'] = chunk.apply(self.parse_datetime, axis=1)
                    self.logger.debug(f"Created 'datetime' column in chunk {chunk_number}.")
                else:
                    self.logger.warning(f"Missing 'date' or 'time' columns in chunk {chunk_number}.")
                    if progress_callback:
                        progress_callback(f"Missing 'date' or 'time' columns in chunk {chunk_number}.")

                # Rename 'time-taken' to 'time_taken_ms' for clarity
                if 'time-taken' in chunk.columns:
                    chunk.rename(columns={'time-taken': 'time_taken_ms'}, inplace=True)
                    self.logger.debug(f"Renamed 'time-taken' to 'time_taken_ms' in chunk {chunk_number}.")

                # # If user-specified columns, keep only those that actually exist
                # if selected_columns:
                #     existing_cols = [c for c in selected_columns if c in chunk.columns]
                #     chunk = chunk[existing_cols]

                # Log the columns present in the chunk for debugging
                self.logger.debug(f"Columns in chunk {chunk_number}: {list(chunk.columns)}")

                # Report progress
                if progress_callback:
                    progress_callback(f"Processed chunk {chunk_number} of {log_file_path}")

                yield chunk

        except Exception as e:
            self.logger.error(f"Error parsing {log_file_path}: {e}")
            if progress_callback:
                progress_callback(f"Error parsing {log_file_path}: {e}")
            return

    def generate_advanced_text_report(
        self,
        log_identifier,
        total_requests,
        avg_tt,
        max_tt,
        requests_by_method,
        requests_by_status,
        slow_requests_count,
        df_4xx_count,
        df_5xx_count,
        top_slowest_requests,
        big_download_tail=False
    ):
        """
        Generates a detailed textual report based on analysis.
        """
        self.logger.debug(f"Generating advanced text report for {log_identifier}")
        lines = []
        lines.append("=======================================================================")
        lines.append(f"ADVANCED IIS LOG ANALYSIS REPORT FOR: {log_identifier}")
        lines.append("=======================================================================")
        lines.append("")
        lines.append(f"Total Requests: {total_requests:,}")
        if avg_tt is not None:
            lines.append(f"Average Time Taken (ms): {avg_tt:.0f}")  # Rounded to whole number
        if max_tt is not None:
            lines.append(f"Maximum Time Taken (ms): {int(max_tt)}")
        lines.append("")

        # Summarize methods
        if not requests_by_method.empty:
            top_m = requests_by_method.idxmax()
            top_m_count = requests_by_method.max()
            lines.append(f"Most used HTTP method: '{top_m}' ({int(top_m_count):,} calls).")
        lines.append("")

        # Summarize status
        if not requests_by_status.empty:
            lines.append("Top Status Codes:")
            for s, c in requests_by_status.items():
                if pd.notna(s) and pd.notna(c):
                    lines.append(f"  {int(s)}: {int(c)}")
                else:
                    lines.append(f"  N/A: N/A")
        else:
            lines.append("Top Status Codes: N/A")
        lines.append("")

        # Slow requests
        if slow_requests_count > 0:
            lines.append(f"Detected {slow_requests_count:,} requests taking longer than threshold.")
            lines.append("This may indicate large file downloads or server-side performance issues.")
        else:
            lines.append("No requests exceeded the slow threshold time.")
        lines.append("")

        # 4xx and 5xx
        lines.append("Error Analysis:")
        lines.append(f" - 4xx errors: {df_4xx_count:,}")
        lines.append(f" - 5xx errors: {df_5xx_count:,}")
        lines.append("")

        # Explanation about distribution
        lines.append("**TimeTaken Distribution Insights**:")
        lines.append(" - Often, there's a main peak around a low value (e.g., under 100 ms) if the server uses caching.")
        lines.append(" - A secondary peak (e.g., 200-300 ms or more) can appear for uncached or more complex requests.")
        if big_download_tail:
            lines.append(" - We observe a 'long tail' at higher values (seconds), likely due to big file downloads.")
            lines.append("   The time to send big files depends on client-server bandwidth, not just server speed.")
        lines.append("")

        # BytesSent Analysis
        if big_download_tail:
            lines.append("**BytesSent Analysis**:")
            lines.append(" - Large BytesSent values correlate with big file downloads, inflating TimeTaken.")
            lines.append(" - Filtering out requests with BytesSent < 1 MB shows the server's 'real' response times.")
            lines.append("")

        lines.append("End of advanced report.")
        report = "\n".join(lines)
        self.logger.debug("Advanced text report generated successfully.")
        return report

    def analyze_logs(
        self,
        file_paths,
        mode='single',
        output_path=None,
        interruption_flag=None,
        progress_callback=None,
        extra_params=None
    ):
        """
        Analyzes IIS logs based on the selected mode and exports the results to an Excel file.

        Args:
            file_paths (list of str): List of log file paths to analyze.
            mode (str): Mode of analysis - 'single', 'cluster', or 'multiple'.
            output_path (str): Path to save the Excel report.
            interruption_flag (callable, optional): Function that returns True if interruption is requested.
            progress_callback (callable, optional): Function to report progress messages.
            extra_params (dict, optional): Additional user-defined parameters, e.g.:
                {
                    'slow_request_threshold_ms': 5000,
                    'selected_columns': [...],
                    'generate_advanced_report': True/False,
                    ...
                }

        Returns:
            str or None: Path to the generated Excel file or None if analysis failed or was canceled.
        """
        # Default the extra_params if none provided
        if extra_params is None:
            extra_params = {}

        self.logger.info(f"Starting analysis in '{mode}' mode with files: {file_paths}")
        self.logger.debug(f"extra_params: {extra_params}")

        # Extract user parameters from extra_params
        slow_threshold = extra_params.get('slow_request_threshold_ms', 5000)
        user_selected_cols = extra_params.get('selected_columns', None)
        generate_advanced = extra_params.get('generate_advanced_report', True)

        # Validate mode
        if mode not in ['single', 'cluster', 'multiple']:
            err_msg = f"Invalid mode '{mode}'. Choose from 'single', 'cluster', or 'multiple'."
            self.logger.error(err_msg)
            if progress_callback:
                progress_callback(err_msg)
            return None

        # Validate file counts based on mode
        if mode == 'single' and len(file_paths) != 1:
            err_msg = "Single mode requires exactly one file."
            self.logger.error(err_msg)
            if progress_callback:
                progress_callback(err_msg)
            return None
        elif mode == 'cluster' and len(file_paths) != 2:
            err_msg = "Cluster mode requires exactly two files."
            self.logger.error(err_msg)
            if progress_callback:
                progress_callback(err_msg)
            return None
        elif mode == 'multiple' and len(file_paths) < 2:
            err_msg = "Multiple mode requires at least two files."
            self.logger.error(err_msg)
            if progress_callback:
                progress_callback(err_msg)
            return None

        # Initialize aggregation variables
        total_requests = 0
        requests_by_method = pd.Series(dtype='int')
        requests_by_status = pd.Series(dtype='int')
        slow_requests_count = 0
        df_4xx_count = 0
        df_5xx_count = 0
        top_slowest_requests = pd.DataFrame()
        avg_tt_sum = 0.0
        avg_tt_count = 0
        max_tt = 0.0
        avg_tt_by_hour = pd.Series(dtype='float')
        top_ips = pd.Series(dtype='int')
        top_uris = pd.Series(dtype='int')

        total_files = len(file_paths)
        current_file = 0

        # Loop over each file, read in chunks, and aggregate
        for fp in file_paths:
            current_file += 1
            self.logger.debug(f"Processing file: {fp} ({current_file}/{total_files})")
            if progress_callback:
                progress_callback(f"Processing file {current_file} of {total_files}: {os.path.basename(fp)}")

            # Read chunks from this file
            for chunk in self.load_log_file_in_chunks(
                fp,
                interruption_flag=interruption_flag,
                progress_callback=progress_callback,
                selected_columns=user_selected_cols  # pass chosen columns
            ):
                # 1) Aggregate total requests
                total_requests += len(chunk)

                # 2) requests by method
                if 'cs-method' in chunk.columns:
                    method_counts = chunk['cs-method'].value_counts(dropna=False)
                    requests_by_method = requests_by_method.add(method_counts, fill_value=0)
                    self.logger.debug(f"Aggregated methods: {method_counts.to_dict()}")
                    if progress_callback:
                        progress_callback(f"Aggregated methods in chunk: {method_counts.to_dict()}")

                # 3) requests by status
                if 'sc-status' in chunk.columns:
                    status_counts = chunk['sc-status'].value_counts(dropna=False)
                    requests_by_status = requests_by_status.add(status_counts, fill_value=0)
                    self.logger.debug(f"Aggregated statuses: {status_counts.to_dict()}")
                    if progress_callback:
                        progress_callback(f"Aggregated statuses in chunk: {status_counts.to_dict()}")

                # 4) slow requests
                if 'time_taken_ms' in chunk.columns:
                    # Use user-provided threshold rather than hardcoded 5000
                    slow_requests = chunk[chunk['time_taken_ms'] > slow_threshold]
                    slow_requests_count += len(slow_requests)
                    self.logger.debug(
                        f"Found {len(slow_requests)} slow requests in current chunk "
                        f"(threshold={slow_threshold}ms)."
                    )
                    if progress_callback:
                        progress_callback(f"Found {len(slow_requests)} slow requests in chunk.")

                    # Update top 10 slowest
                    top_slowest_requests = pd.concat([top_slowest_requests, slow_requests])
                    top_slowest_requests = top_slowest_requests.nlargest(10, 'time_taken_ms')

                    # Aggregate average + max time
                    avg_tt_sum += slow_requests['time_taken_ms'].sum()
                    avg_tt_count += slow_requests['time_taken_ms'].count()
                    current_max_tt = slow_requests['time_taken_ms'].max()
                    if pd.notna(current_max_tt) and current_max_tt > max_tt:
                        max_tt = current_max_tt
                        if progress_callback:
                            progress_callback(f"Updated maximum time taken to {max_tt} ms.")

                # 5) 4xx & 5xx errors
                if 'sc-status' in chunk.columns:
                    df_4xx = chunk[(chunk['sc-status'] >= 400) & (chunk['sc-status'] < 500)]
                    df_5xx = chunk[chunk['sc-status'] >= 500]
                    df_4xx_count += len(df_4xx)
                    df_5xx_count += len(df_5xx)
                    self.logger.debug(f"Aggregated 4xx: {df_4xx_count}, 5xx: {df_5xx_count}")
                    if progress_callback:
                        progress_callback(
                            f"Aggregated 4xx and 5xx errors: 4xx={df_4xx_count}, 5xx={df_5xx_count}"
                        )

                # 6) average time taken by hour
                if 'datetime' in chunk.columns and 'time_taken_ms' in chunk.columns:
                    # drop rows where datetime or time_taken_ms is NaN
                    chunk = chunk.dropna(subset=['datetime', 'time_taken_ms'])
                    if not chunk.empty:
                        chunk.set_index('datetime', inplace=True)
                        # resample hourly
                        avg_tt_hour = chunk['time_taken_ms'].resample('h').mean().round().astype(int).rename("AvgTTbyHour (ms)")
                        # add to global aggregator
                        avg_tt_by_hour = avg_tt_by_hour.add(avg_tt_hour, fill_value=0)
                        self.logger.debug("Aggregated average Time Taken by hour.")
                        if progress_callback:
                            progress_callback("Aggregated average Time Taken by hour.")

                # 7) top IPs
                if 'c-ip' in chunk.columns:
                    ip_counts = chunk['c-ip'].value_counts()
                    top_ips = top_ips.add(ip_counts, fill_value=0)
                    self.logger.debug("Aggregated Top IPs.")
                    if progress_callback:
                        progress_callback("Aggregated Top IPs.")

                # 8) top URIs
                if 'cs-uri-stem' in chunk.columns:
                    uri_counts = chunk['cs-uri-stem'].value_counts()
                    top_uris = top_uris.add(uri_counts, fill_value=0)
                    self.logger.debug("Aggregated Top URIs.")
                    if progress_callback:
                        progress_callback("Aggregated Top URIs.")

        # Check for interruption after reading all files
        if interruption_flag and interruption_flag():
            self.logger.info("Analysis was interrupted before finalizing.")
            if progress_callback:
                progress_callback("Analysis was interrupted before finalizing.")
            return None

        # If we somehow didn't load any data
        if not any([total_requests, len(requests_by_method), len(requests_by_status)]):
            msg = "No valid data loaded from the selected files."
            self.logger.error(msg)
            if progress_callback:
                progress_callback(msg)
            return None

        # Identify log name
        if mode in ['cluster', 'multiple']:
            log_identifier = " & ".join([os.path.basename(fp) for fp in file_paths])
        else:
            log_identifier = os.path.basename(file_paths[0])

        self.logger.debug(
            "Final aggregated data => "
            f"Requests={total_requests}, Methods={len(requests_by_method)}, "
            f"Statuses={len(requests_by_status)}"
        )
        if progress_callback:
            progress_callback("Final aggregation completed.")

        # Calculate overall average time taken among slow requests
        avg_tt = avg_tt_sum / avg_tt_count if avg_tt_count > 0 else None

        # Actually write the final analysis to Excel
        return self.perform_analysis(
            log_identifier=log_identifier,
            total_requests=total_requests,
            avg_tt=avg_tt,
            max_tt=max_tt,
            requests_by_method=requests_by_method,
            requests_by_status=requests_by_status,
            slow_requests_count=slow_requests_count,
            df_4xx_count=df_4xx_count,
            df_5xx_count=df_5xx_count,
            top_slowest_requests=top_slowest_requests,
            avg_tt_by_hour=avg_tt_by_hour,
            top_ips=top_ips,
            top_uris=top_uris,
            interruption_flag=interruption_flag,
            output_path=output_path,
            progress_callback=progress_callback,
            generate_advanced_report=generate_advanced  # Pass this down
        )

    def perform_analysis(
        self,
        log_identifier,
        total_requests,
        avg_tt,
        max_tt,
        requests_by_method,
        requests_by_status,
        slow_requests_count,
        df_4xx_count,
        df_5xx_count,
        top_slowest_requests,
        avg_tt_by_hour,
        top_ips,
        top_uris,
        interruption_flag=None,
        output_path=None,
        progress_callback=None,
        generate_advanced_report=True
    ):
        """
        Performs analysis on the aggregated data and writes results to an Excel file.

        Args:
            log_identifier (str): Identifier for the logs (e.g., filename or cluster name).
            total_requests (int): Total number of requests.
            avg_tt (float or None): Average time taken in ms (only among the "slow" subset).
            max_tt (float or None): Maximum time taken in ms.
            requests_by_method (pd.Series): Counts of HTTP methods.
            requests_by_status (pd.Series): Counts of status codes.
            slow_requests_count (int): Number of requests taking longer than threshold.
            df_4xx_count (int): Number of 4xx errors.
            df_5xx_count (int): Number of 5xx errors.
            top_slowest_requests (pd.DataFrame): DataFrame of top slowest requests.
            avg_tt_by_hour (pd.Series): Sum of average Time Taken per hour.
            top_ips (pd.Series): Top IP addresses.
            top_uris (pd.Series): Top URIs.
            interruption_flag (callable, optional): Function that returns True if interruption is requested.
            output_path (str): Path to save the Excel report.
            progress_callback (callable, optional): Function to report progress messages.
            generate_advanced_report (bool): If False, skip writing the AdvancedReport sheet.

        Returns:
            str or None: Path to the generated Excel file or None if analysis failed or was canceled.
        """
        self.logger.info(f"Performing analysis for '{log_identifier}'")
        if progress_callback:
            progress_callback("Starting report generation.")

        # Check for interruption
        if interruption_flag and interruption_flag():
            self.logger.info("Analysis was interrupted before performing analysis.")
            if progress_callback:
                progress_callback("Analysis was interrupted before performing analysis.")
            return None

        # Possibly generate advanced text report
        if generate_advanced_report:
            big_download_tail = False  # Placeholder logic
            adv_report = self.generate_advanced_text_report(
                log_identifier=log_identifier,
                total_requests=total_requests,
                avg_tt=avg_tt,
                max_tt=max_tt,
                requests_by_method=requests_by_method,
                requests_by_status=requests_by_status,
                slow_requests_count=slow_requests_count,
                df_4xx_count=df_4xx_count,
                df_5xx_count=df_5xx_count,
                top_slowest_requests=top_slowest_requests,
                big_download_tail=big_download_tail
            )
            adv_report_lines = adv_report.split("\n")
            adv_report_df = pd.DataFrame({'AdvancedReport': adv_report_lines})
            self.logger.debug("Generated advanced text report.")
            if progress_callback:
                progress_callback("Generated advanced text report.")
        else:
            adv_report_df = pd.DataFrame({'AdvancedReport': ["(Advanced report skipped.)"]})
            self.logger.debug("User chose not to generate advanced report.")

        # Generate a Basic Report
        lines = []
        lines.append("==== BASIC IIS Log Analysis ====")
        lines.append(f"Log Identifier: {log_identifier}")
        lines.append(f"Total Requests: {total_requests:,}")
        if avg_tt is not None:
            lines.append(f"Average Time Taken (ms) among slow requests: {avg_tt:.0f}")
        else:
            lines.append("Average Time Taken (ms) among slow requests: N/A")
        if max_tt is not None:
            lines.append(f"Maximum Time Taken (ms): {int(max_tt)}")
        else:
            lines.append("Maximum Time Taken (ms): N/A")
        lines.append("")
        if not requests_by_method.empty:
            lines.append("Top Methods:")
            for m, c in requests_by_method.items():
                if pd.notna(m) and pd.notna(c):
                    lines.append(f"  {m}: {int(c)}")
                else:
                    lines.append("  N/A: N/A")
        else:
            lines.append("Top Methods: N/A")
        lines.append("")
        if not requests_by_status.empty:
            lines.append("Top Status Codes:")
            for s, c in requests_by_status.items():
                if pd.notna(s) and pd.notna(c):
                    lines.append(f"  {int(s)}: {int(c)}")
                else:
                    lines.append("  N/A: N/A")
        else:
            lines.append("Top Status Codes: N/A")
        lines.append("")
        if not top_slowest_requests.empty:
            lines.append("Slowest Requests (Top 10):")
            for idx, row in top_slowest_requests.head(10).iterrows():
                uri = row.get('cs-uri-stem', 'N/A')
                time_taken = row.get('time_taken_ms', 'N/A')
                ip = row.get('c-ip', 'N/A')
                status = row.get('sc-status', 'N/A')
                lines.append(f"  {uri} => {time_taken} ms, IP={ip}, status={status}")
        else:
            lines.append("Slowest Requests (Top 10): N/A")

        # DataFrames for final output
        basic_report_df = pd.DataFrame({'Report': lines})
        adv_report_df = pd.DataFrame({'AdvancedReport': adv_report.split("\n")}) if adv_report else None

        summary_stats = {
            'Total Requests': total_requests,
            'Average Time Taken (ms)': f"{avg_tt:.0f}" if avg_tt is not None else "N/A",
            'Maximum Time Taken (ms)': int(max_tt) if max_tt is not None else "N/A",
            '4xx Errors': df_4xx_count,
            '5xx Errors': df_5xx_count
        }
        summary_df = pd.DataFrame(list(summary_stats.items()), columns=['Metric', 'Value'])

        try:
            if progress_callback:
                progress_callback("Writing analysis results to Excel...")

            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Basic sheets
                if not requests_by_method.empty:
                    requests_by_method.to_frame('Count').to_excel(writer, sheet_name='RequestsByMethod')
                if not requests_by_status.empty:
                    requests_by_status.to_frame('Count').to_excel(writer, sheet_name='RequestsByStatus')
                summary_df.to_excel(writer, sheet_name='SummaryStats', index=False)
                if df_4xx_count > 0 or df_5xx_count > 0:
                    error_summary = {
                        'Error Type': ['4xx Errors', '5xx Errors'],
                        'Count': [df_4xx_count, df_5xx_count]
                    }
                    error_df = pd.DataFrame(error_summary)
                    error_df.to_excel(writer, sheet_name='ErrorReport', index=False)  # Changed to 'ErrorReport'
                if slow_requests_count > 0:
                    slow_requests_df = top_slowest_requests.copy()
                    slow_requests_df.to_excel(writer, sheet_name='SlowRequests', index=False)  # Changed to 'SlowRequests'
                else:
                    self.logger.info(f"slow requests {slow_requests_df}")
                # Additional sheets
                if not avg_tt_by_hour.empty:
                    avg_tt_by_hour_df = avg_tt_by_hour.to_frame(name='AvgTTbyHour (ms)')
                    avg_tt_by_hour_df.to_excel(writer, sheet_name='AvgTTbyHour', index=True)
                if not top_ips.empty:
                    top_ips = top_ips.sort_values(ascending=False).astype(int)
                    top_ips.to_frame('Request Count').to_excel(writer, sheet_name='TopIPs')
                if not top_uris.empty:
                    top_uris = top_uris.sort_values(ascending=False).astype(int)
                    top_uris.to_frame('Request Count').to_excel(writer, sheet_name='TopURIs')

                # CorrelationMatrix is skipped due to lack of complete data

                # Textual reports
                basic_report_df.to_excel(writer, sheet_name='BasicReport', index=False)
                if generate_advanced_report and adv_report_df is not None:
                    adv_report_df.to_excel(writer, sheet_name='AdvancedReport', index=False)
                else:
                    # If generate_advanced_report=False, we'll still store the single-line DF
                    adv_report_df.to_excel(writer, sheet_name='AdvancedReport', index=False)

                # Basic formatting examples
                # Adjust column widths
                # (the dictionary of worksheets is in writer.sheets after each .to_excel call)
                if 'AdvancedReport' in writer.sheets:
                    writer.sheets['AdvancedReport'].set_column('A:A', 100)
                if 'BasicReport' in writer.sheets:
                    writer.sheets['BasicReport'].set_column('A:A', 100)
                if 'SummaryStats' in writer.sheets:
                    writer.sheets['SummaryStats'].set_column('A:A', 30)
                    writer.sheets['SummaryStats'].set_column('B:B', 30)
                if 'TopIPs' in writer.sheets:
                    writer.sheets['TopIPs'].set_column('A:A', 20)  # IP col
                    writer.sheets['TopIPs'].set_column('B:B', 15)  # Count col
                if 'TopURIs' in writer.sheets:
                    writer.sheets['TopURIs'].set_column('A:A', 30)
                    writer.sheets['TopURIs'].set_column('B:B', 15)
                # Add more formatting as needed

            self.logger.info(f"Analysis + Detailed Report saved to '{output_path}'")
            if progress_callback:
                progress_callback(f"Analysis complete. Report saved to '{output_path}'")
            return output_path  # Return path to generated Excel file

        except Exception as e:
            msg = f"Failed to write Excel report: {e}"
            self.logger.error(msg, exc_info=True)
            if progress_callback:
                progress_callback(msg)
            return None
