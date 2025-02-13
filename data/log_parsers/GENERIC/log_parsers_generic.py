import os
import re
import logging
from datetime import datetime
from collections import defaultdict
from lxml import etree  # Needed for XML parsing

# Import functions from your msg_parser module
from data.log_parsers.GENERIC.msg_parser import (
    parse_big_xml,         # used for full XML files
    merge_transaction_info  # used within line-based logs
)
# Import specialized prom parser
from data.log_parsers.GENERIC.prom_parser import parse_prom_log

logger = logging.getLogger("GenericLogParser")

###############################################################################
# Regex for line-based logs
###############################################################################
# Example bracket log:
# [2025-01-03 14:18:32,399] [0x000018ec] [DEBUG] [LpeComm] - [<LogLine File= "LpeComm.cpp" Line= "93"><![CDATA[GetCommStatus: 1]]></LogLine>]
RE_BRACKET_LINE = re.compile(
    r'^\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\]\s+\[0x[a-fA-F0-9]+\]\s+\[([A-Za-z]+)\]\s+\[(.*?)\]\s+-\s+\[(.*)\]$'
)
# Example ISO log:
# 2025-02-11 18:24:51,061 - MainWindow - INFO - Opening Generic Log for Parsing: C:/Users/Pc/Downloads/File.log.1
RE_ISO_LINE = re.compile(
    r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s*-\s*(.*?)\s*-\s*([A-Z]+)\s*-\s*(.*)$'
)
# New regex for converter-style log lines (e.g. in lpeconverter.log)
# Example:
# 2025-01-29 02:14:53,261 DEBUG [0:8888:9999:1:723048509:1/29/2025 2:14:49 AM] [27] LPE                    PromSrvClient.Send                           - Before, PromSrv Process GetLoyaltySummary Message 
RE_CONVERTER_LINE = re.compile(
    r'^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})\s+([A-Z]+)\s+\[([^\]]+)\]\s+\[([^\]]+)\]\s+(\S+)\s+(\S+)\s+-\s+(.*)$'
)

# For ignoring LPE query lines
RE_LPE_QUERY = re.compile(r'<LPE\s+Method="Query', re.IGNORECASE)
RE_LPE_QUERY_RESPONSE = re.compile(r'<LPE\s+Method="Query\(Response\)', re.IGNORECASE)

###############################################################################
# Time parsing helper (for line-based logs)
###############################################################################
def parse_datetime(dt_str, fmt='%Y-%m-%d %H:%M:%S,%f'):
    """Convert bracket/ISO log line timestamps to datetime."""
    try:
        return datetime.strptime(dt_str, fmt)
    except ValueError:
        try:
            return datetime.strptime(dt_str, "%d/%m/%y %H:%M:%S.%f")
        except ValueError:
            return None

###############################################################################
# Helper to process inline XML from a log message.
# This version accumulates subsequent lines if the inline XML is not complete.
###############################################################################
def process_inline_xml_from_line(message_fragment, file_iterator, transactions, source_file):
    """
    If the message fragment appears to contain XML (i.e. it contains '<?xml' or starts with '<'),
    this function attempts to accumulate subsequent lines if necessary (e.g. if <![CDATA[ is used)
    and then processes the entire XML fragment using msg_parser's parse_single_xml().
    """
    if "<?xml" not in message_fragment and not message_fragment.strip().startswith("<"):
        return

    xml_fragment = message_fragment
    # If a CDATA start is found but the closing is missing, accumulate more lines.
    if "<![CDATA[" in xml_fragment and "]]>" not in xml_fragment:
        for next_line in file_iterator:
            xml_fragment += "\n" + next_line.rstrip("\n")
            if "]]>" in next_line:
                break

    # Remove any <![CDATA[ ... ]]> wrappers.
    xml_fragment = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', xml_fragment, flags=re.DOTALL).strip()
    try:
        subroot = etree.fromstring(xml_fragment.encode('utf-8', errors='replace'))
        # Import parse_single_xml on demand.
        from data.log_parsers.GENERIC.msg_parser import parse_single_xml
        parse_single_xml(subroot, transactions, source_file=source_file)
        logger.debug("Processed inline XML fragment from log lines.")
    except Exception as e:
        logger.debug(f"Failed to process inline XML: {e}")

###############################################################################
# Main line-based log parser
###############################################################################
def parse_line_based(filepath, transactions, source_file=None):
    """
    Line-based log parser for standard logs that may also contain inline XML fragments.
    For each line, it extracts the timestamp (if present), merges transaction info,
    and calls process_inline_xml_from_line() to handle any inline XML (accumulating multiple lines if needed).
    Also, if a line has no timestamp but looks like XML, we inherit the previous timestamp.
    """
    last_ts = None
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        # Use the file object as an iterator to support multiline XML accumulation.
        for line in f:
            line = line.rstrip('\n')

            # Skip LPE <Query lines.
            if RE_LPE_QUERY.search(line) or RE_LPE_QUERY_RESPONSE.search(line):
                continue

            dt_obj = None
            level = None
            message_part = None
            contains_xml = False

            # 1) Check if this is a bracket log line.
            m_b = RE_BRACKET_LINE.match(line)
            if m_b:
                dt_str, lvl, src, message_part = m_b.groups()
                dt_obj = parse_datetime(dt_str)
                level = lvl
                last_ts = dt_obj if dt_obj else last_ts
                if message_part and ("<?xml" in message_part or message_part.strip().startswith("<")):
                    contains_xml = True
                merge_transaction_info(line, transactions)
                process_inline_xml_from_line(message_part, f, transactions, source_file)
                yield {
                    'raw_line': line,
                    'combined_ts': dt_obj.timestamp() if dt_obj else None,
                    'log_level': level,
                    'trans_ids': [],
                    'source_file': source_file,
                    'is_transaction': contains_xml
                }
                continue

            # 2) Check if this is a converter-style log line.
            m_c = RE_CONVERTER_LINE.match(line)
            if m_c:
                dt_str, lvl, field1, field2, src, func, message_part = m_c.groups()
                dt_obj = parse_datetime(dt_str)
                level = lvl
                last_ts = dt_obj if dt_obj else last_ts
                if message_part and ("<?xml" in message_part or message_part.strip().startswith("<")):
                    contains_xml = True
                merge_transaction_info(line, transactions)
                process_inline_xml_from_line(message_part, f, transactions, source_file)
                yield {
                    'raw_line': line,
                    'combined_ts': dt_obj.timestamp() if dt_obj else None,
                    'log_level': level,
                    'trans_ids': [],
                    'source_file': source_file,
                    'is_transaction': contains_xml
                }
                continue

            # 3) Check if this is an ISO log line.
            m_i = RE_ISO_LINE.match(line)
            if m_i:
                dt_str, src, lvl, message_part = m_i.groups()
                dt_obj = parse_datetime(dt_str)
                level = lvl
                last_ts = dt_obj if dt_obj else last_ts
                if message_part and ("<?xml" in message_part or message_part.strip().startswith("<")):
                    contains_xml = True
                merge_transaction_info(line, transactions)
                process_inline_xml_from_line(message_part, f, transactions, source_file)
                yield {
                    'raw_line': line,
                    'combined_ts': dt_obj.timestamp() if dt_obj else None,
                    'log_level': level,
                    'trans_ids': [],
                    'source_file': source_file,
                    'is_transaction': contains_xml
                }
                continue

            # 4) Fallback: plain line.
            merge_transaction_info(line, transactions)
            # If the line looks like XML and we have a previous timestamp, inherit it.
            if line.lstrip().startswith("<") and last_ts:
                dt_obj = last_ts
                contains_xml = True
            process_inline_xml_from_line(line, f, transactions, source_file)
            yield {
                'raw_line': line,
                'combined_ts': dt_obj.timestamp() if dt_obj else None,
                'log_level': None,
                'trans_ids': [],
                'source_file': source_file,
                'is_transaction': contains_xml
            }

###############################################################################
# Parse single file (auto-detect line-based vs. XML)
###############################################################################
def parse_generic_log(filepath, source_file=None):
    """
    Single-file parse function.
      1) If the file name matches "prom.*.xml", call the specialized prom parser.
      2) Otherwise, if the file’s content looks like pure XML (e.g. starting with '<?xml'),
         use the XML parser (parse_big_xml); if not, use line-based parsing.
    Returns: (all_rows, min_dt, max_dt, transactions)
    """
    transactions = {}
    all_rows = []
    min_dt = None
    max_dt = None

    base_name = os.path.basename(filepath).lower()

    # If this is a dedicated PROM file, use the prom parser.
    if re.match(r'^prom.*\.xml$', base_name):
        logger.debug(f"Detected prom-xml file: {filepath}; using specialized parser.")
        transactions = parse_prom_log(filepath)
        row_iter = []
        for txid, tx_data in transactions.items():
            ts = tx_data.get('transaction_time')
            ts_float = ts.timestamp() if ts is not None else None
            row_iter.append({
                'raw_line': f"[PROMFILE] TX={txid}",
                'combined_ts': ts_float,
                'log_level': None,
                'trans_ids': [txid],
                'source_file': source_file,
                'is_transaction': True
            })
    else:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as ftest:
            first_chunk = ftest.read(2048)
            ftest.seek(0)
        # If the file chunk starts with '<?xml' (or contains a root tag), assume it’s a full XML file.
        if first_chunk.lstrip().startswith("<?xml") or '<Root' in first_chunk or '<Customer' in first_chunk:
            row_iter = parse_big_xml(filepath, transactions, source_file=source_file)
        else:
            row_iter = parse_line_based(filepath, transactions, source_file=source_file)

    # Collect rows and compute overall min and max timestamps.
    for row in row_iter:
        all_rows.append(row)
        ts = row.get('combined_ts')
        if ts is not None:
            if min_dt is None or ts < min_dt:
                min_dt = ts
            if max_dt is None or ts > max_dt:
                max_dt = ts

    return all_rows, min_dt, max_dt, transactions

###############################################################################
# Parse multiple logs and combine
###############################################################################
def parse_multiple_logs(file_list):
    """
    Parse multiple files, combining their raw log rows and aggregated transaction data.
    """
    all_combined_rows = []
    global_transactions = {}
    global_min = None
    global_max = None

    for fpath in file_list:
        these_rows, min_dt, max_dt, these_trans = parse_generic_log(
            fpath, source_file=os.path.basename(fpath)
        )
        all_combined_rows.extend(these_rows)
        for txid, tdata in these_trans.items():
            if txid not in global_transactions:
                global_transactions[txid] = tdata
            else:
                # Optionally merge additional details here.
                pass
        if min_dt is not None:
            if global_min is None or min_dt < global_min:
                global_min = min_dt
        if max_dt is not None:
            if global_max is None or max_dt > global_max:
                global_max = max_dt

    return all_combined_rows, global_min, global_max, global_transactions
