# msg_parser.py

import re
import codecs
from datetime import datetime
from lxml import etree

###############################################################################
# Regex for capturing transaction data in raw text (used when merging info)
###############################################################################
RE_TRANS_ID = re.compile(r'\b(?:TransID|TransactionNumber|TicketNumber)\s*=\s*"([^"]+)"', re.IGNORECASE)
RE_CARD_ID  = re.compile(r'\bCardID\s*=\s*"([^"]+)"', re.IGNORECASE)
RE_PHONE    = re.compile(r'\bName="(?:MobilePhoneNumber|HomePhone)"\s+Value="(\d+)"', re.IGNORECASE)
RE_FIRST    = re.compile(r'\bFirstName\s*=\s*"([^"]+)"', re.IGNORECASE)
RE_LAST     = re.compile(r'\bLastName\s*=\s*"([^"]+)"', re.IGNORECASE)
RE_PROMO    = re.compile(r'\b(?:Promotion\s+ID|PromNumber|PromotionID)="?(\d+)"?', re.IGNORECASE)

# For scanning <ItemInfo ...> in raw lines:
RE_ITEM_BLOCK = re.compile(r'<ItemInfo\s+([^>]+)>', re.IGNORECASE)
RE_ATTR       = re.compile(r'(\w+)\s*=\s*"([^"]+)"')

# Inline XML timestamps (some known attributes)
RE_START_DATETIME = re.compile(r'StartDateTime="([^"]+)"')
RE_END_DATETIME   = re.compile(r'EndDateTime="([^"]+)"')
RE_BUSINESS_DATE  = re.compile(r'BusinessDate="([^"]+)"')

###############################################################################
# Timestamp parser for XML fields
###############################################################################
def parse_timestamp(ts_str):
    """
    Tries several common ISO or T‑separated timestamp formats.
    Supports both comma and dot as the fractional separator.
    """
    for fmt in (
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S,%f',
        '%Y-%m-%d %H:%M:%S'
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            pass
    return None

###############################################################################
# Transaction initialization
###############################################################################
def init_transaction(txid, transactions):
    """Initialize a transaction record if it does not already exist."""
    if txid not in transactions:
        transactions[txid] = {
            'trans_id': txid,
            'storeID': None,
            'cashierID': None,
            'card_id': None,
            'first_name': None,
            'last_name': None,
            'phone_numbers': set(),
            'promotions': set(),
            'items': [],
            'documents': [],
            'tenders': [],
            'msg_type_counts': {},
            'transaction_time': None,  # earliest found timestamp
            'explicit_total': None,
            'promo_items': [],
            'loyalty_info': {
                'balances': [],
                'accounts': [],
                'members': []
            },
            'timestamps': set()  # store all discovered timestamps
        }

###############################################################################
# Helper to merge from text (line-based or inline chunk)
###############################################################################
def merge_transaction_info(text, transactions):
    """
    Scans 'text' for known patterns (TransID, CardID, etc.) and merges data into `transactions`.
    Also extracts known inline timestamps from attributes like StartDateTime, EndDateTime, etc.
    """
    trans_ids = RE_TRANS_ID.findall(text)
    if not trans_ids:
        return

    for txid in set(trans_ids):
        init_transaction(txid, transactions)
        rec = transactions[txid]

        # --- Basic attributes (card, names, phones, promotions) ---
        for c in RE_CARD_ID.findall(text):
            if not rec['card_id']:
                rec['card_id'] = c
        firsts = RE_FIRST.findall(text)
        lasts  = RE_LAST.findall(text)
        if firsts and not rec['first_name']:
            rec['first_name'] = firsts[-1]
        if lasts and not rec['last_name']:
            rec['last_name'] = lasts[-1]
        for p in RE_PHONE.findall(text):
            rec['phone_numbers'].add(p)
        for pm in RE_PROMO.findall(text):
            rec['promotions'].add(pm)

        # --- Items from <ItemInfo ...> blocks ---
        for block_str in RE_ITEM_BLOCK.findall(text):
            attrs = dict(RE_ATTR.findall(block_str))
            rec['items'].append({
                'plu': attrs.get('PluCode', ''),
                'name': attrs.get('Name', '').strip(),
                'depCode': attrs.get('DepCode', ''),
                'qty': float(attrs.get('Quantity', '1')),
                'price': float(attrs.get('Price', '0')),
                'amount': float(attrs.get('Amount', '0')),
            })

        # --- Extract timestamps from certain attributes in the text ---
        for regex in (RE_START_DATETIME, RE_END_DATETIME, RE_BUSINESS_DATE):
            m = regex.search(text)
            if m:
                ts = parse_timestamp(m.group(1).strip())
                if ts:
                    rec['timestamps'].add(ts)

        if rec['timestamps'] and rec['transaction_time'] is None:
            rec['transaction_time'] = min(rec['timestamps'])

###############################################################################
# Recursively scan for *any* date/time tags or attributes we care about
###############################################################################
def scan_for_timestamps(element, aggregator):
    """
    Recursively scans 'element' and its children for known date/time 
    attributes or tag names (e.g. StartDateTime, EndDateTime, BusinessDate, 
    ExpirationDate, etc.) and adds them to aggregator['timestamps'].
    """
    DATE_ATTRS = {
        'StartDateTime', 'EndDateTime', 'BusinessDate', 'ServerDate',
        'PeriodBusinessDate', 'MemberEffectiveDate', 'ExpirationDate',
        'StartDate', 'EndDate'
    }
    # 1) Check element's attributes
    for attr_name, attr_value in element.attrib.items():
        if attr_name in DATE_ATTRS:
            dt = parse_timestamp(attr_value)
            if dt:
                aggregator['timestamps'].add(dt)

    # 2) Check element text if tag is one of our date/time fields
    tag_name = element.tag
    if tag_name in DATE_ATTRS and element.text:
        dt = parse_timestamp(element.text.strip())
        if dt:
            aggregator['timestamps'].add(dt)

    # 3) Recurse into children
    for child in element:
        scan_for_timestamps(child, aggregator)

###############################################################################
# Loyalty-specific parsing
###############################################################################
def parse_loyalty_balances(root, aggregator):
    """
    Finds <Balance> or <Acc> elements to store in aggregator['balances'] or
    aggregator['accounts'].
    """
    balances = root.findall('.//Balance')
    for b in balances:
        aggregator['balances'].append({
            'type': b.get('Type'),
            'balance_id': b.get('ID'),
            'name': b.get('Name'),
            'open_balance': b.get('OpenBalance'),
            'earnings': b.get('Earnings'),
            'redemptions': b.get('Redemptions'),
            'current_balance': b.get('CurrentBalance'),
        })

    accs = root.findall('.//Acc')
    for a in accs:
        aggregator['accounts'].append({
            'acc_id': a.get('ID'),
            'earn_value': a.get('EarnValue'),
            'open_balance': a.get('OpenBalance'),
            'ending_balance': a.get('EndingBalance'),
            'value': a.get('Value'),
        })

def parse_loyalty_members(root, aggregator):
    """
    Finds <Member ...> elements to store in aggregator['members'].
    """
    members = root.findall('.//Member')
    for m in members:
        mem_data = {
            'last_name': m.get('LastName'),
            'first_name': m.get('FirstName'),
            'status': m.get('Status'),
            'member_external_id': m.get('MemberExternalId'),
        }
        aggregator['members'].append(mem_data)

def parse_loyalty_xml(fragment, aggregator):
    """
    Higher-level function to parse loyalty elements in 'fragment' and
    store them in aggregator['balances'], aggregator['accounts'], aggregator['members'].
    """
    parse_loyalty_balances(fragment, aggregator)
    parse_loyalty_members(fragment, aggregator)

###############################################################################
# Main XML parser for a single root
###############################################################################
def parse_single_xml(root, transactions, source_file=None):
    """
    Given an XML 'root', examine known sections (<Log><Session>, <biztalk_1>, <Customer>, etc.)
    and harvest data into 'transactions'. 
    Calls 'merge_transaction_info(...)' and 'scan_for_timestamps(...)'
    to capture all data and timestamps.
    """
    # PART 1: Process <Session> if present
    session = root.find('.//Session')
    if session is not None:
        current_ticket = None
        for lpe in session.findall('LPE'):
            method = lpe.get('Method')
            xml_str = "".join(lpe.itertext()).strip()
            xml_str = re.sub(r'<\?xml\s+.*?\?>', '', xml_str, flags=re.DOTALL).strip()
            try:
                subroot = etree.fromstring(xml_str.encode('utf-8', errors='replace'))
            except Exception:
                subroot = None
            if method == 'SetParam' and subroot is not None:
                sysparams = subroot.find('.//SystemParameters')
                if sysparams is not None:
                    current_ticket = sysparams.get('TicketNumber')
                    store_id = sysparams.get('StoreID')
                    cashier_id = sysparams.get('CashierID')
                    if current_ticket:
                        init_transaction(current_ticket, transactions)
                        rec = transactions[current_ticket]
                        rec['storeID'] = store_id
                        rec['cashierID'] = cashier_id
                        scan_for_timestamps(subroot, rec)
                        if rec['transaction_time'] is None and rec['timestamps']:
                            rec['transaction_time'] = min(rec['timestamps'])
            elif method == 'AddItem' and subroot is not None and current_ticket:
                rec = transactions[current_ticket]
                item_info = subroot.find('.//ItemInfo')
                if item_info is not None:
                    plu = item_info.get('PluCode', '')
                    nm  = item_info.get('Name', '').strip()
                    dep = item_info.get('DepCode', '')
                    amt_val = float(item_info.get('Amount','0') or 0)
                    qty_val = float(item_info.get('Quantity','1') or 1)
                    base_price = float(item_info.get('Price','0') or 0)
                    prices_el = item_info.find('.//Prices/Price')
                    subprice_val = float(prices_el.get('Price','0')) if prices_el is not None else base_price
                    rec['items'].append({
                        'plu': plu,
                        'name': nm,
                        'depCode': dep,
                        'qty': qty_val,
                        'price': subprice_val,
                        'amount': amt_val,
                    })
                scan_for_timestamps(subroot, rec)
                if rec['transaction_time'] is None and rec['timestamps']:
                    rec['transaction_time'] = min(rec['timestamps'])
            elif method in ('AddTender','AddDocument','AddDocument(Response)') and subroot is not None and current_ticket:
                rec = transactions[current_ticket]
                if method == 'AddTender':
                    tend_el = subroot.find('.//TenderInfo')
                    if tend_el is not None:
                        amt = float(tend_el.get('Amount','0') or 0)
                        rec['tenders'].append({
                            'tenderNo': tend_el.get('TenderNo'),
                            'amount': amt,
                            'tenderType': tend_el.get('TenderType') or ''
                        })
                else:
                    docinfo = subroot.find('.//DocumentInfo')
                    if docinfo is not None:
                        for d in docinfo.findall('Document'):
                            rec['documents'].append({
                                'documentType': d.get('DocumentType'),
                                'barcode': d.get('Barcode'),
                                'confirmationLevel': d.get('ConfirmationLevel'),
                                'promotionId': d.get('PromotionId'),
                                'description': d.get('PromotionDescription')
                            })
                scan_for_timestamps(subroot, rec)
                if rec['transaction_time'] is None and rec['timestamps']:
                    rec['transaction_time'] = min(rec['timestamps'])
            elif method == 'GetTriggeredPromotions' and subroot is not None and current_ticket:
                rec = transactions[current_ticket]
                for dl in subroot.findall('.//DiscountLine'):
                    pn = dl.get('PromNumber')
                    if pn:
                        rec['promotions'].add(pn)
                scan_for_timestamps(subroot, rec)
                if rec['transaction_time'] is None and rec['timestamps']:
                    rec['transaction_time'] = min(rec['timestamps'])
            elif method == 'Query(Response)' and subroot is not None:
                titems_node = subroot.find('.//TicketItems')
                if titems_node is not None:
                    if not current_ticket:
                        gdata = subroot.find('.//GeneralData')
                        if gdata is not None:
                            tnum = gdata.get('TicketNumber')
                            if tnum:
                                current_ticket = tnum
                                init_transaction(tnum, transactions)
                    if current_ticket:
                        rec = transactions[current_ticket]
                        for iel in titems_node.findall('Item'):
                            pl   = iel.get('PluCode','')
                            dep  = iel.get('DepCode','')
                            qty  = float(iel.get('Quantity','1') or 1)
                            price= float(iel.get('Price','0') or 0)
                            rew  = float(iel.get('RewardAmount','0') or 0)
                            if rew == 0:
                                rew = price*qty
                            rec['items'].append({
                                'plu': pl,
                                'name': '',
                                'depCode': dep,
                                'qty': qty,
                                'price': price,
                                'amount': rew
                            })
                        parse_loyalty_xml(subroot, rec['loyalty_info'])
                        scan_for_timestamps(subroot, rec)
                        if rec['transaction_time'] is None and rec['timestamps']:
                            rec['transaction_time'] = min(rec['timestamps'])
            merge_transaction_info(xml_str, transactions)

    # --------------------------------------------------------------------------
    # PART 2: <biztalk_1> branch
    # --------------------------------------------------------------------------
    biztalk = root.find('.//biztalk_1')
    if biztalk is not None:
        body = biztalk.find('body')
        if body is not None:
            ast = body.find('ActiveStore_SalesTransaction_1.70')
            if ast is not None:
                tx_number  = ast.findtext('TransactionNumber')
                store_id   = ast.findtext('StoreID')
                cashier_id = ast.findtext('CashierID')
                if tx_number:
                    init_transaction(tx_number, transactions)
                    rec = transactions[tx_number]
                    rec['storeID'] = store_id
                    rec['cashierID'] = cashier_id
                    scan_for_timestamps(ast, rec)
                    if rec['transaction_time'] is None and rec['timestamps']:
                        rec['transaction_time'] = min(rec['timestamps'])
                    total_el = ast.find('.//TotalAmount')
                    if total_el is not None:
                        try:
                            rec['explicit_total'] = float(total_el.text or 0)
                        except:
                            pass
                    for tdet in ast.findall('.//TransactionDetail'):
                        group = tdet.find('TransactionDetailGroup')
                        if group is not None:
                            for line_el in group.findall('TransactionDetailLine'):
                                promo_id = line_el.findtext('PromotionID')
                                if promo_id:
                                    rec['promotions'].add(promo_id)
                                mid = line_el.findtext('MarkdownItemID')
                                depc = line_el.findtext('MarkdownDepartmentID')
                                if mid and depc:
                                    qty_text = (line_el.findtext('TriggeredQty') or line_el.findtext('AllocatedQty') or '1')
                                    amt_text = (line_el.findtext('Amount') or '0')
                                    try:
                                        qty_val = float(qty_text)
                                    except:
                                        qty_val = 1.0
                                    try:
                                        amt_val = float(amt_text)
                                    except:
                                        amt_val = 0.0
                                    rec['items'].append({
                                        'plu': mid,
                                        'name': '(markdown item)',
                                        'depCode': depc,
                                        'qty': qty_val,
                                        'price': 0.0,
                                        'amount': amt_val
                                    })
                    for psum in ast.findall('.//PromotionSummary'):
                        p_id = psum.findtext('RedeemedPromotionId')
                        if p_id:
                            rec['promotions'].add(p_id)
                    parse_loyalty_xml(ast, rec['loyalty_info'])
    # --------------------------------------------------------------------------
    # PART 3: <Customer ...> branch
    # --------------------------------------------------------------------------
    # If the root itself is a Customer, process it.
    def is_customer(elem):
        return elem.tag.endswith('Customer')
    if is_customer(root):
        customers = [root]
    else:
        customers = root.findall('.//Customer')
    for cust in customers:
        txid = cust.get('TransID')
        if not txid:
            continue
        init_transaction(txid, transactions)
        rec = transactions[txid]
        cardid = cust.get('CardID')
        if cardid:
            rec['card_id'] = cardid
        ttot = cust.get('TicketTotal')
        if ttot:
            try:
                rec['explicit_total'] = float(ttot)
            except:
                pass
        scan_for_timestamps(cust, rec)
        if rec['transaction_time'] is None and rec['timestamps']:
            rec['transaction_time'] = min(rec['timestamps'])
        xml_str = etree.tostring(cust, encoding=str)
        merge_transaction_info(xml_str, transactions)
        try:
            subroot = etree.fromstring(xml_str.encode('utf-8', errors='replace'))
            parse_loyalty_xml(subroot, rec['loyalty_info'])
        except Exception:
            pass

###############################################################################
# Attempts to parse a file as XML, either single-root or multiple top-level chunks
###############################################################################
def parse_big_xml(filepath, transactions, source_file=None):
    """
    1) Reads the entire file (strips BOM if present).
    2) Tries to parse as a single XML root (fromstring). If that fails with 
       multiple-root issues, parse each top-level <...> chunk in a fallback loop.
    3) For each parsed root, calls parse_single_xml(...) to harvest data into 'transactions'.
    4) Yields rows (dicts) with 'combined_ts' set to the earliest discovered tx time in that chunk.
    """
    with open(filepath, 'rb') as f:
        data = f.read()
    BOM_UTF8 = codecs.BOM_UTF8
    if data.startswith(BOM_UTF8):
        data = data[len(BOM_UTF8):]
    def earliest_new_tx_time(before_keys):
        earliest_dt = None
        after_keys = set(transactions.keys())
        new_keys = after_keys - before_keys
        for k in new_keys:
            dt = transactions[k].get('transaction_time')
            if dt and (earliest_dt is None or dt < earliest_dt):
                earliest_dt = dt
        return earliest_dt
    text_str = data.decode('utf-8', errors='replace').strip()
    before_keys = set(transactions.keys())
    try:
        root = etree.fromstring(data)
        parse_single_xml(root, transactions, source_file=source_file)
        chunk_dt = earliest_new_tx_time(before_keys)
        chunk_ts = chunk_dt.timestamp() if chunk_dt else None
        yield {
            'raw_line': etree.tostring(root, encoding=str),
            'combined_ts': chunk_ts,
            'log_level': None,
            'trans_ids': list(transactions.keys()),
            'source_file': source_file
        }
        return
    except Exception as e:
        print(f"[parse_big_xml] Single-root parse failed for {filepath}: {e}")
        pass
    idx = 0
    length = len(text_str)
    while idx < length:
        while idx < length and text_str[idx].isspace():
            idx += 1
        if idx >= length or text_str[idx] != '<':
            break
        start_idx = idx
        bracket_count = 1
        idx += 1
        while idx < length:
            open_tag = text_str.find("<", idx)
            close_tag = text_str.find(">", idx)
            if close_tag == -1:
                idx = length
                break
            if open_tag == -1 or open_tag > close_tag:
                bracket_count -= 1
                idx = close_tag + 1
                if bracket_count == 0:
                    break
            else:
                bracket_count += 1
                idx = open_tag + 1
        chunk = text_str[start_idx:idx].strip()
        before_keys_chunk = set(transactions.keys())
        try:
            subroot = etree.fromstring(chunk.encode('utf-8', errors='replace'))
            parse_single_xml(subroot, transactions, source_file=source_file)
            chunk_dt = earliest_new_tx_time(before_keys_chunk)
            chunk_ts = chunk_dt.timestamp() if chunk_dt else None
            yield {
                'raw_line': chunk,
                'combined_ts': chunk_ts,
                'log_level': None,
                'trans_ids': list(transactions.keys()),
                'source_file': source_file
            }
        except Exception as e:
            print(f"[parse_big_xml] Chunk parse error in file {filepath}: {e}")
            pass
