# # data/log_parsers/EVTX/log_parsers_evtx.py

import re
from datetime import datetime
import logging
import lxml.etree as ET  # using lxml for XML parsing
import json

from evtx import PyEvtxParser  # pylint: disable=no-name-in-module # type: ignore # Rust-based parser

logger = logging.getLogger("LogParsersEVTX") #pylint: disable=no-member
logger.setLevel(logging.DEBUG) #pylint: disable=no-member

def parse_evtx_log(filepath):
    logger.info(f"Starting EVTX log parsing for file: {filepath}")
    rows = []
    min_time = None
    max_time = None

    try:
        parser = PyEvtxParser(filepath)
        record_count = 0
        for record in parser.records():
            record_count += 1
            if record_count % 1000 == 0:
                logger.info(f"Parsing record number: {record_count}")
            try:
                xml = record["data"]
                event = parse_evtx_record_xml(xml)
                if event:
                    rows.append(event)
                    ts_epoch = event.get("timestamp_epoch")
                    if ts_epoch:
                        if (min_time is None) or (ts_epoch < min_time):
                            min_time = ts_epoch
                            logger.debug(f"Updated min_time to: {min_time}")
                        if (max_time is None) or (ts_epoch > max_time):
                            max_time = ts_epoch
                            logger.debug(f"Updated max_time to: {max_time}")
            except Exception as e:
                logger.warning(f"Failed to parse a record at record {record_count}: {e}")
        logger.info(f"Completed parsing EVTX file: {filepath}. Total records parsed: {record_count}")
    except Exception as e:
        logger.error(f"Error opening or parsing EVTX '{filepath}': {e}")
    logger.info(f"Parsed {len(rows)} rows from EVTX log '{filepath}'.")
    return rows, min_time, max_time

def parse_evtx_record_xml(xml_str):
    try:
        if not xml_str.lstrip().startswith("<?xml"):
            xml_str = '<?xml version="1.0" encoding="utf-8"?>\n' + xml_str
        root = ET.fromstring(xml_str.encode("utf-8")) # type: ignore
        event = {}
        ns = "{http://schemas.microsoft.com/win/2004/08/events/event}"
        
        # System section
        system = root.find(ns + "System")
        if system is not None:
            event["EventID"] = system.findtext(ns + "EventID", "Unknown")
            event["RecordNumber"] = system.findtext(ns + "EventRecordID", "")
            time_elem = system.find(ns + "TimeCreated")
            time_str = time_elem.get("SystemTime") if time_elem is not None else ""
            event["timestamp_epoch"] = parse_timestamp(time_str)
            event["timestamp"] = time_str
            provider_elem = system.find(ns + "Provider")
            event["ProviderName"] = provider_elem.get("Name") if provider_elem is not None else "Unknown"
            event["Level"] = system.findtext(ns + "Level", "Unknown")
            event["Channel"] = system.findtext(ns + "Channel", "Unknown")
            event["Computer"] = system.findtext(ns + "Computer", "Unknown")
        
        # EventData section – use the proper namespace for children
        event_data = {}
        data_texts = []
        event_data_elem = root.find(ns + "EventData")
        if event_data_elem is not None:
            for data in event_data_elem.findall(ns + "Data"):
                name = data.get("Name", "Unnamed")
                value = " ".join(data.itertext()).strip()
                key = re.sub(r'\W+', '_', name)
                event_data[key] = value
                if value:
                    data_texts.append(value)
        # Save full JSON and display text
        event["EventData"] = json.dumps(event_data)
        event["EventData_display"] = "\n".join(data_texts)
        event["raw_xml"] = xml_str
        return event
    except ET.XMLSyntaxError as pe:
        logger.error(f"XML parsing error: {pe}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during XML parsing: {e}")
        return None

def parse_timestamp(time_str):
    if not time_str or len(time_str) < 23:
        return None
    try:
        return datetime(
            int(time_str[0:4]),
            int(time_str[5:7]),
            int(time_str[8:10]),
            int(time_str[11:13]),
            int(time_str[14:16]),
            int(time_str[17:19]),
            int(time_str[20:23]) * 1000
        ).timestamp()
    except Exception:
        return None
