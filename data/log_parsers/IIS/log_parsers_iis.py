# log_parsers/log_parsers_iis.py

import logging
import re
from datetime import datetime
from dateutil import parser as date_parser

logger = logging.getLogger('IISLogParser')  # pylint: disable=no-member
logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

def parse_iis_log_generator(file_obj):
    """
    Generator that parses an IIS log file line by line.
    Yields dictionaries representing each log entry.
    """
    logger.info("Starting IIS log parsing.")
    
    try:
        field_list = []
        current_date_for_time = None
        min_time = None
        max_time = None

        for line_num, line in enumerate(file_obj, start=1):
            line = line.strip()
            if not line:
                continue

            # Handle comments and metadata
            if line.startswith("#"):
                if line.lower().startswith("#fields:"):
                    # Example: "#Fields: date time s-ip cs-method cs-uri-stem ..."
                    fields_line = line.split(":", 1)[1].strip()
                    # Replace hyphens and parentheses with underscores for consistency
                    field_list = [re.sub(r'[\-()]', '_', field) for field in fields_line.split()]
                    logger.debug(f"Fields parsed on line {line_num}: {field_list}")
                elif line.lower().startswith("#date:"):
                    # Example: "#Date: 2025-01-08 03:15:51"
                    date_str = line.split(":", 1)[1].strip()
                    try:
                        dt = date_parser.parse(date_str)
                        current_date_for_time = dt
                        logger.debug(f"Current date for time set to: {current_date_for_time}")
                    except Exception as e:
                        current_date_for_time = None
                        logger.warning(f"Failed to parse date on line {line_num}: {e}")
                # Ignore other # lines
                continue

            # If we don't have a #Fields line yet, skip
            if not field_list:
                logger.warning(f"No #Fields line found before data on line {line_num}. Skipping line.")
                continue

            # Split the line into parts, considering quoted fields
            parts = re.findall(r'(?:"[^"]*"|\S)+', line)
            if len(parts) < len(field_list):
                # Append '-' for missing fields
                parts += ['-'] * (len(field_list) - len(parts))
                logger.warning(f"Line {line_num} has fewer fields ({len(parts)}) than expected ({len(field_list)}). Appending '-' for missing fields.")
            elif len(parts) > len(field_list):
                # Trim extra fields
                parts = parts[:len(field_list)]
                logger.warning(f"Line {line_num} has more fields ({len(parts)}) than expected ({len(field_list)}). Trimming extra fields.")

            # Build a dict for this log line with underscore-based keys
            row_dict = {}
            for col_name, value in zip(field_list, parts):
                row_dict[col_name] = value

            # Attempt to build timestamp
            combined_ts = None

            if "date" in field_list and "time" in field_list:
                d = row_dict.get("date", "")
                t = row_dict.get("time", "")
                if d and t:
                    try:
                        dt_str = f"{d} {t}"
                        combined_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        combined_ts = combined_dt.timestamp()
                        logger.debug(f"Timestamp parsed from date and time on line {line_num}: {combined_ts}")
                    except ValueError as ve:
                        combined_ts = None
                        logger.warning(f"Failed to parse datetime on line {line_num}: {ve}")
            elif "time" in field_list:
                t_str = row_dict.get("time", "")
                if t_str:
                    try:
                        t_obj = datetime.strptime(t_str, "%H:%M:%S").time()
                        if current_date_for_time:
                            combined_dt = datetime.combine(current_date_for_time.date(), t_obj) # type: ignore
                        else:
                            combined_dt = datetime.combine(datetime.now().date(), t_obj)
                        combined_ts = combined_dt.timestamp()
                        logger.debug(f"Timestamp parsed from time on line {line_num}: {combined_ts}")
                    except ValueError as ve:
                        combined_ts = None
                        logger.warning(f"Failed to parse time on line {line_num}: {ve}")
            elif "date" in field_list:
                d_str = row_dict.get("date", "")
                if d_str:
                    try:
                        combined_dt = datetime.strptime(d_str, "%Y-%m-%d")
                        combined_ts = combined_dt.timestamp()
                        logger.debug(f"Timestamp parsed from date on line {line_num}: {combined_ts}")
                    except ValueError as ve:
                        combined_ts = None
                        logger.warning(f"Failed to parse date on line {line_num}: {ve}")

            if combined_ts:
                # Update min_time and max_time
                if min_time is None or combined_ts < min_time:
                    min_time = combined_ts
                    logger.debug(f"Updated min_time to: {min_time}")
                if max_time is None or combined_ts > max_time:
                    max_time = combined_ts
                    logger.debug(f"Updated max_time to: {max_time}")

                row_dict["combined_ts"] = combined_ts
            else:
                row_dict["combined_ts"] = None

            # Add raw_line for full log line
            row_dict["raw_line"] = line

            yield row_dict

    except Exception as e:
        logger.error(f"Error parsing IIS log: {e}")
