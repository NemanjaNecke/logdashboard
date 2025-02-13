
import re
import logging
import hashlib
from lxml import etree
from datetime import datetime
from copy import deepcopy

logger = logging.getLogger(__name__)


def extract_timestamp(root):
    # 1. Try Session/StartTime (expected format: Date="28/01/25", Time="10:07:02.292")
    st_elem = root.find('.//Session/StartTime')
    if st_elem is not None:
        date_str = st_elem.get('Date')
        time_str = st_elem.get('Time')
        if date_str and time_str:
            try:
                # Adjust the format if needed (here: day/month/yy H:M:S.micro)
                return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%y %H:%M:%S.%f")
            except Exception as e:
                # log or ignore parsing errors
                pass

    # 2. Try the Customer element’s StartDateTime attribute (ISO format, e.g. "2025-01-28T10:07:01")
    cust_elem = root.find('.//Customer')
    if cust_elem is not None:
        start_dt = cust_elem.get('StartDateTime')
        if start_dt:
            try:
                # If the value is in ISO format, datetime.fromisoformat works well:
                return datetime.fromisoformat(start_dt)
            except Exception as e:
                pass

    # 3. Try the GeneralData element in Query(Response)
    gd_elem = root.find('.//GeneralData')
    if gd_elem is not None:
        trans_date = gd_elem.get('TransactionDate')
        trans_time = gd_elem.get('TransactionTime')
        if trans_date and trans_time:
            try:
                # Assuming TransactionDate is "28/01/2025" and TransactionTime "10:07:01"
                return datetime.strptime(f"{trans_date} {trans_time}", "%d/%m/%Y %H:%M:%S")
            except Exception as e:
                pass

    # Fallback: no timestamp found.
    return None

###############################################################################
# 1) Helper: Clean XML declarations as before
###############################################################################
def clean_xml_declarations(content: str) -> str:
    declarations = re.findall(r'<\?xml.*?\?>', content, flags=re.DOTALL)
    if not declarations:
        return content
    first_decl = declarations[0]
    content = re.sub(r'<\?xml.*?\?>', '', content, flags=re.DOTALL)
    return first_decl + "\n" + content.strip()

###############################################################################
# 2) Instead of splitting via regex, stream the file by wrapping it in a root.
###############################################################################
def iterparse_prom_xml(filepath: str):
    """
    Reads the file, cleans XML declarations, wraps it in a synthetic <Logs> root,
    and yields each <LPE> element using lxml.
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    # Clean extra XML declarations.
    content = clean_xml_declarations(content)
    # Wrap the content in a synthetic root so the entire file is well-formed.
    wrapped = "<Logs>\n" + content + "\n</Logs>"
    try:
        # Parse the wrapped content.
        parser = etree.XMLParser(recover=True, remove_blank_text=True)
        root = etree.fromstring(wrapped.encode('utf-8'), parser=parser)
    except etree.XMLSyntaxError as e:
        logger.error(f"Error parsing wrapped XML: {e}")
        return
    # Yield each LPE element.
    for lpe_elem in root.findall('.//LPE'):
        yield lpe_elem

###############################################################################
# 3) Transaction aggregator functions (init_transaction, etc.)
###############################################################################
def init_transaction(txid, transactions):
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
            # New dictionary to keep track of item keys for deduplication:
            'item_keys': {},
            'documents': [],
            'tenders': [],
            'promo_items': [],
            'transaction_time': None,
            'explicit_total': None,
            'loyalty_info': {
                'balances': [],
                'accounts': [],
                'members': [],
                'segments': []
            },
            'loyalty_summary': None,
            'savers_summary': None,
            'queries': [],
            'raw_fragment_attributes': [],
            'promotion_details': [],
            'other_fragments': [],
            # Optional: to avoid processing duplicate fragments:
            'processed_fragments': set()
        }

###############################################################################
# 4) Loyalty sub-parsing and attribute extraction (unchanged)
###############################################################################
def extract_all_attributes(element):
    all_attrs = {}
    for elem in element.iter():
        tag = elem.tag
        if elem.attrib:
            all_attrs.setdefault(tag, []).append(dict(elem.attrib))
    return all_attrs

def merge_extra_info(element, current_tx, transactions):
    # Capture CardID if available.
    card_ids = element.xpath('.//@CardID')
    if card_ids:
        transactions[current_tx]['card_id'] = card_ids[0]
    # Capture phone numbers (if any)
    phones = element.xpath('.//@MobilePhoneNumber') + element.xpath('.//@HomePhone')
    for num in phones:
        transactions[current_tx]['phone_numbers'].add(num)
    # Also capture names if available
    fn = element.get('FirstName')
    ln = element.get('LastName')
    if fn:
        transactions[current_tx]['first_name'] = fn
    if ln:
        transactions[current_tx]['last_name'] = ln
    return

###############################################################################
# 5) --- New helper: Compute a unique key for an item.
###############################################################################
def get_item_key(item_info):
    # Use the key attributes you consider uniquely identifying an item.
    plu = item_info.get('PluCode', '').strip()
    pos_seq = item_info.get('PosSequence', '').strip()
    dep = item_info.get('DepCode', '').strip()
    key_str = f"{plu}_{pos_seq}_{dep}"
    return hashlib.sha256(key_str.encode('utf-8')).hexdigest()
    # Alternatively, simply: return key_str

###############################################################################
# 5) Revised LPE Fragment Processor – collects ALL information!
###############################################################################
def process_lpe_fragment(fragment, method, transactions, current_tx=None):
    # Compute a hash for this fragment so that duplicates are skipped.
    fragment_raw = etree.tostring(fragment, encoding='utf-8') # type: ignore
    frag_hash = hashlib.sha256(fragment_raw).hexdigest()
    if current_tx and frag_hash in transactions[current_tx].get('processed_fragments', set()):
        logger.debug(f"Skipping duplicate fragment with hash {frag_hash} in transaction {current_tx}")
        return current_tx
    if current_tx:
        transactions[current_tx].setdefault('processed_fragments', set()).add(frag_hash)

    # ----- Branch: SetParam (initialize transaction) -----
    if method == 'SetParam':
        sysparams = fragment.find('.//SystemParameters')
        if sysparams is not None:
            txid = sysparams.get('TicketNumber')
            if txid:
                init_transaction(txid, transactions)
                transactions[txid]['storeID'] = sysparams.get('StoreID')
                transactions[txid]['cashierID'] = sysparams.get('CashierID')
                merge_extra_info(sysparams, txid, transactions)
                current_tx = txid
                transactions[txid]['raw_fragment_attributes'].append(extract_all_attributes(sysparams))
                transactions[txid].setdefault('other_fragments', []).append({
                    'method': method,
                    'raw': etree.tostring(sysparams, encoding='unicode')
                })
                return current_tx

    # ----- Branch: Init -----
    elif method == 'Init' and current_tx:
        init_info = fragment.find('.//InitInfo')
        if init_info is not None:
            transactions[current_tx]['init_info'] = dict(init_info.attrib)
            active_devices = init_info.find('.//ActiveDevices')
            if active_devices is not None:
                devices = [dict(ad.attrib) for ad in active_devices.findall('ActiveDevice')]
                transactions[current_tx]['active_devices'] = devices
            merge_extra_info(init_info, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(init_info))

    # ----- Branch: AddItem -----
    elif method == 'AddItem' and current_tx:
        item_info = fragment.find('.//ItemInfo')
        if item_info is not None:
            rec = transactions[current_tx]
            key = get_item_key(item_info)
            plu  = item_info.get('PluCode', '')
            name = item_info.get('Name', '').strip()
            dep  = item_info.get('DepCode', '')
            pos_seq = item_info.get('PosSequence', None)
            qty_str   = item_info.get('Quantity', '1')
            amt_str   = item_info.get('Amount', '0')
            price_str = item_info.get('Price', '0')
            qip_str   = item_info.get('QuantityInPrice', '1')
            try:
                qty_val = float(qty_str)
            except ValueError:
                qty_val = 1.0
            try:
                amt_val = float(amt_str)
            except ValueError:
                amt_val = 0.0
            try:
                base_price = float(price_str)
            except ValueError:
                base_price = 0.0
            try:
                qip_val = float(qip_str)
            except ValueError:
                qip_val = 1.0
            prices_el = item_info.find('.//Prices/Price')
            if prices_el is not None:
                raw_subprice = prices_el.get('Price', '0')
                try:
                    subprice_val = float(raw_subprice)
                except ValueError:
                    subprice_val = base_price
            else:
                subprice_val = base_price
            final_price = subprice_val
            final_qty   = qty_val
            final_amt   = amt_val
            item_data = {
                'plu': plu,
                'name': name,
                'depCode': dep,
                'posSequence': pos_seq,
                'qty': final_qty,
                'price': final_price,
                'amount': final_amt,
                'quantity_in_price': qip_val,
                'raw': etree.tostring(item_info, encoding='unicode'),
                'key': key
            }
            if 'item_keys' not in rec:
                rec['item_keys'] = {}
            if key in rec['item_keys']:
                idx = rec['item_keys'][key]
                rec['items'][idx]['qty'] += final_qty
                rec['items'][idx]['amount'] += final_amt
                logger.debug(f"Merged item with key {key} (PLU {plu}) in transaction {current_tx}")
            else:
                rec['items'].append(item_data)
                rec['item_keys'][key] = len(rec['items']) - 1
                logger.debug(f"Added new item with key {key} (PLU {plu}) in transaction {current_tx}")
            merge_extra_info(item_info, current_tx, transactions)
            rec.setdefault('raw_fragment_attributes', []).append(extract_all_attributes(item_info))

    # ----- Branch: Query(Response) -----
    elif method == 'Query(Response)':
        gdata = fragment.find('.//GeneralData')
        if gdata is not None:
            tnum = gdata.get('TicketNumber')
            if tnum:
                init_transaction(tnum, transactions)
                current_tx = tnum
                merge_extra_info(gdata, current_tx, transactions)
                transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(gdata))
        titems_node = fragment.find('.//TicketItems')
        if titems_node is not None and current_tx:
            for iel in titems_node.findall('Item'):
                pl = iel.get('PluCode', '')
                dep = iel.get('DepCode', '')
                try:
                    qty = float(iel.get('Quantity', '1'))
                except ValueError:
                    qty = 1.0
                try:
                    price = float(iel.get('Price', '0'))
                except ValueError:
                    price = 0.0
                try:
                    rew_amt = float(iel.get('RewardAmount', '0'))
                except ValueError:
                    rew_amt = 0.0
                if rew_amt == 0.0:
                    rew_amt = price * qty
                merged = False
                for existing in transactions[current_tx]['items']:
                    if existing.get('plu') == pl and existing.get('depCode') == dep:
                        existing['qty'] += qty
                        existing['amount'] += rew_amt
                        merged = True
                        break
                if not merged:
                    transactions[current_tx]['items'].append({
                        'plu': pl,
                        'name': '',
                        'depCode': dep,
                        'qty': qty,
                        'price': price,
                        'amount': rew_amt,
                        'raw': etree.tostring(iel, encoding='unicode')
                    })
            merge_extra_info(titems_node, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(titems_node))
        loyalty = fragment.find('.//LoyaltyInfo')
        if loyalty is not None and current_tx:
            parse_loyalty_xml(loyalty, transactions[current_tx]['loyalty_info'])
            transactions[current_tx]['loyalty_summary'] = etree.tostring(loyalty, encoding='unicode')
        merge_extra_info(fragment, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Membership updates (AddMmbrCard, AddMmbrInfo) -----
    elif method in ('AddMmbrCard', 'AddMmbrInfo') and current_tx:
        loyalty = fragment.find('.//LoyaltyInfo')
        if loyalty is not None:
            parse_loyalty_xml(loyalty, transactions[current_tx]['loyalty_info'])
            transactions[current_tx]['loyalty_summary'] = etree.tostring(loyalty, encoding='unicode')
            merge_extra_info(loyalty, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(loyalty))
        else:
            transactions[current_tx].setdefault('other_fragments', []).append({
                'method': method,
                'raw': etree.tostring(fragment, encoding='unicode')
            })
            merge_extra_info(fragment, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Tenders and Documents -----
    elif method in ('AddTender', 'AddDocument', 'AddDocument(Response)') and current_tx:
        if method == 'AddTender':
            tender_el = fragment.find('.//TenderInfo')
            if tender_el is not None:
                try:
                    amount = float(tender_el.get('Amount', '0'))
                except ValueError:
                    amount = 0.0
                transactions[current_tx]['tenders'].append({
                    'tenderNo': tender_el.get('TenderNo'),
                    'amount': amount,
                    'tenderType': tender_el.get('TenderType') or ''
                })
                merge_extra_info(tender_el, current_tx, transactions)
                transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(tender_el))
        else:
            docinfo = fragment.find('.//DocumentInfo')
            if docinfo is not None:
                for d in docinfo.findall('Document'):
                    transactions[current_tx]['documents'].append({
                        'documentType': d.get('DocumentType'),
                        'barcode': d.get('Barcode'),
                        'confirmationLevel': d.get('ConfirmationLevel'),
                        'promotionId': d.get('PromotionId'),
                        'description': d.get('PromotionDescription'),
                        'raw': etree.tostring(d, encoding='unicode')
                    })
            docs_resp = fragment.find('.//Documents')
            if docs_resp is not None:
                for d in docs_resp.findall('Document'):
                    transactions[current_tx]['documents'].append({
                        'documentType': d.get('DocumentType'),
                        'barcode': d.get('Barcode'),
                        'confirmationLevel': d.get('ConfirmationLevel'),
                        'promotionId': d.get('PromotionId'),
                        'description': d.get('PromotionDescription'),
                        'raw': etree.tostring(d, encoding='unicode')
                    })
            merge_extra_info(fragment, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Triggered Promotions -----
    elif method.startswith('GetTriggeredPromotions') and current_tx:
        for dl in fragment.findall('.//DiscountLine'):
            pn = dl.get('PromNumber')
            if pn:
                transactions[current_tx]['promotions'].add(pn)
            transactions[current_tx].setdefault('promo_items', []).append(dict(dl.attrib))
        merge_extra_info(fragment, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Query (for requests) -----
    elif method == 'Query' and current_tx:
        query_elem = fragment.find('.//PromQuery')
        if query_elem is not None:
            transactions[current_tx].setdefault('queries', []).append(
                etree.tostring(query_elem, encoding='unicode')
            )
        merge_extra_info(fragment, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Loyalty Summary -----
    elif method.startswith('GetLoyaltySummary') and current_tx:
        loyalty_info = fragment.find('.//LoyaltyInfo')
        if loyalty_info is not None:
            parse_loyalty_xml(loyalty_info, transactions[current_tx]['loyalty_info'])
            transactions[current_tx]['loyalty_summary'] = etree.tostring(loyalty_info, encoding='unicode')
        merge_extra_info(fragment, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Branch: Savers Summary -----
    elif method.startswith('GetSaversSummary') and current_tx:
        savers_info = etree.tostring(fragment, encoding='unicode')
        transactions[current_tx]['savers_summary'] = savers_info
        merge_extra_info(fragment, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Catch-all -----
    else:
        if current_tx:
            transactions[current_tx].setdefault('other_fragments', []).append({
                'method': method,
                'raw': etree.tostring(fragment, encoding='unicode')
            })
            merge_extra_info(fragment, current_tx, transactions)
            transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(fragment))

    # ----- Promotion Details (if found anywhere) -----
    promo_details = fragment.find('.//PromotionDetails')
    if promo_details is not None and current_tx:
        details = dict(promo_details.attrib)
        seg_elem = promo_details.find('Segments')
        if seg_elem is not None:
            details['segments'] = [etree.tostring(child, encoding='unicode') for child in seg_elem]
        transactions[current_tx]['promotion_details'].append(details)
        merge_extra_info(promo_details, current_tx, transactions)
        transactions[current_tx]['raw_fragment_attributes'].append(extract_all_attributes(promo_details))
    return current_tx

###############################################################################
# 6) Revised prom*.xml parser with enhanced time parsing
###############################################################################
def parse_prom_log(filepath, transactions=None):
    """
    Parse a prom*.xml file by:
      - Reading the file and cleaning XML declarations,
      - Wrapping it in a synthetic root (<Logs>),
      - Extracting a session start time (if available from <Session>/<StartTime>),
      - Processing each <LPE> element,
      - And if a session start time is found, storing it in the transaction record.
    """
    if transactions is None:
        transactions = {}
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    content = clean_xml_declarations(content)
    wrapped = "<Logs>\n" + content + "\n</Logs>"
    try:
        parser = etree.XMLParser(recover=True, remove_blank_text=True)
        root = etree.fromstring(wrapped.encode('utf-8'), parser=parser)
    except etree.XMLSyntaxError as e:
        logger.error(f"Error parsing wrapped XML: {e}")
        return transactions

    # Extract a timestamp from the XML tree (try several locations)
    session_dt = extract_timestamp(root)
    if session_dt:
        logger.debug(f"Extracted session timestamp: {session_dt.isoformat()}")
    else:
        logger.debug("No timestamp could be extracted from the XML.")

    current_ticket = None
    for lpe_elem in root.findall('.//LPE'):
        method = lpe_elem.get('Method')
        current_ticket = process_lpe_fragment(lpe_elem, method, transactions, current_ticket)
        # If we have a session start time and no transaction_time yet, update it.
        if current_ticket and session_dt and transactions[current_ticket].get('transaction_time') is None:
            transactions[current_ticket]['transaction_time'] = session_dt

    return transactions

###############################################################################
# 7) Loyalty Sub-parsing helpers (unchanged)
###############################################################################
def parse_loyalty_balances(root, aggregator):
    for b in root.findall('.//Balance'):
        aggregator['balances'].append({
            'type': b.get('Type'),
            'balance_id': b.get('ID'),
            'name': b.get('Name'),
            'open_balance': b.get('OpenBalance'),
            'earnings': b.get('Earnings'),
            'redemptions': b.get('Redemptions'),
            'current_balance': b.get('CurrentBalance'),
        })
    for a in root.findall('.//Acc'):
        aggregator['accounts'].append({
            'acc_id': a.get('ID'),
            'earn_value': a.get('EarnValue'),
            'open_balance': a.get('OpenBalance'),
            'ending_balance': a.get('EndingBalance'),
            'value': a.get('Value'),
        })

def parse_loyalty_members(root, aggregator):
    for m in root.findall('.//Member'):
        aggregator['members'].append({
            'last_name': m.get('LastName'),
            'first_name': m.get('FirstName'),
            'status': m.get('Status'),
            'member_external_id': m.get('MemberExternalId'),
        })

def parse_loyalty_xml(fragment, aggregator):
    parse_loyalty_balances(fragment, aggregator)
    parse_loyalty_members(fragment, aggregator)
    segments = fragment.findall('.//Segments/Segment') or fragment.findall('.//Seg')
    for seg in segments:
        aggregator['segments'].append(seg.attrib)
