import os
import sqlite3
import logging
from datetime import datetime

class GenericDBManager:
    """
    Manages SQLite for logs and full aggregator data, including:
      - generic_logs (raw lines)
      - generic_transactions (aggregated: trans_id, card, etc.)
      - generic_items
      - generic_documents
      - generic_tenders
      - generic_promotions
      - generic_msg_types
      - generic_promo_items
      - generic_loyalty_balances
      - generic_loyalty_accounts
      - generic_loyalty_members
      - generic_loyalty_segments
      - generic_loyalty_member_cards
      - generic_loyalty_member_stores
      - plus a metadata table
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger("GenericDBManager")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def init_tables(self):
        """
        Creates all tables needed to store everything from the logs:
          - We store line-based logs in generic_logs
          - Aggregated transaction data in generic_transactions
          - plus items, documents, tenders, promotions, etc.
          - full loyalty info (balances, accounts, members, segments, cards, stores)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            # 1) Raw logs
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                combined_ts REAL,
                log_level TEXT,
                raw_line TEXT,
                trans_ids TEXT,
                source_file TEXT
            )
            """)

            # 2) Aggregated transactions
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT UNIQUE,
                card_id TEXT,
                first_name TEXT,
                last_name TEXT,
                phone_numbers TEXT,
                promotions TEXT,
                item_count INTEGER,
                total_amount REAL,
                transaction_time TEXT
            )
            """)

            # 3) Items
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                plu TEXT,
                name TEXT,
                dep_code TEXT,
                quantity REAL,
                price REAL,
                amount REAL,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 4) Documents
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                document_type TEXT,
                barcode TEXT,
                confirmation_level TEXT,
                promotion_id TEXT,
                description TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 5) Tenders
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                tender_no TEXT,
                amount REAL,
                tender_type TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 6) Promotions
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_promotions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                promotion_id TEXT,
                description TEXT,
                reward_type TEXT,
                reward_amount REAL,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 7) Message type counts
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_msg_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                msg_type TEXT,
                count INTEGER,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 8) Promotion-Items correlation
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_promo_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                promotion_id TEXT,
                item_id TEXT,
                department_id TEXT,
                allocated_qty REAL,
                triggered_qty REAL,
                is_lottery TEXT,
                redeemed_qty REAL,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 9) Loyalty Balances
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_balances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                balance_type TEXT,
                balance_id TEXT,
                name TEXT,
                open_balance TEXT,
                earnings TEXT,
                redemptions TEXT,
                current_balance TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 10) Loyalty Accounts
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                acc_id TEXT,
                value TEXT,
                up_to_date TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 11) Loyalty Members
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                last_name TEXT,
                first_name TEXT,
                status TEXT,
                member_external_id TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 12) Loyalty Segments
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                member_row_id INTEGER,  -- optional link if needed
                segment_id TEXT,
                segment_name TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 13) Loyalty Member Cards
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_member_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                member_row_id INTEGER,
                card_id TEXT,
                card_status TEXT,
                expiration_date TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # 14) Loyalty Member Stores
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_loyalty_member_stores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                member_row_id INTEGER,
                store_id TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            # Extra: metadata
            cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """)
            # Add new tables in init_tables()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_item_taxes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_row_id INTEGER,
                tax_id TEXT,
                tax_amount REAL,
                taxable_amount REAL,
                tax_percent REAL,
                FOREIGN KEY(item_row_id) REFERENCES generic_items(id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_promotion_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trans_id TEXT,
                promotion_id TEXT,
                message_id TEXT,
                message_type TEXT,
                device_type TEXT,
                FOREIGN KEY(trans_id) REFERENCES generic_transactions(trans_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS generic_item_attributes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_row_id INTEGER,
                attr_id TEXT,
                attr_value TEXT,
                FOREIGN KEY(item_row_id) REFERENCES generic_items(id)
            )
            """)

            # Index for timestamps in generic_logs
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_generic_logs_combined_ts 
            ON generic_logs (combined_ts)
            """)

            conn.commit()
            self.logger.info("All database tables initialized successfully.")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error initializing tables: {e}")
        finally:
            conn.close()

    def insert_logs_batch(self, rows):
        """
        Insert a batch of raw logs into generic_logs.
        Each row must have:
          - combined_ts (float or None)
          - log_level
          - raw_line
          - trans_ids (list or str)
          - source_file
        """
        if not rows:
            return
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            data_batch = []
            for r in rows:
                trans_str = r.get('trans_ids', [])
                if isinstance(trans_str, list):
                    trans_str = ",".join(trans_str)
                data_batch.append((
                    r.get('combined_ts', None),
                    r.get('log_level', None),
                    r.get('raw_line',''),
                    trans_str,
                    r.get('source_file','')
                ))

            cur.executemany("""
                INSERT INTO generic_logs (combined_ts, log_level, raw_line, trans_ids, source_file)
                VALUES (?, ?, ?, ?, ?)
            """, data_batch)
            conn.commit()
            self.logger.debug(f"Inserted {len(data_batch)} lines into generic_logs.")
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in insert_logs_batch: {e}")
        finally:
            conn.close()

    def upsert_transaction(self, tx_data):
        """
        Insert or update a transaction in generic_transactions, then store:
          - items
          - documents
          - tenders
          - promotions
          - promo_items correlation
          - loyalty data (balances, accounts, members, segments, cards, stores)
        """
        trans_id = tx_data['trans_id']

        card_id   = tx_data.get('card_id')
        firstn    = tx_data.get('first_name')
        lastn     = tx_data.get('last_name')
        phoneset  = tx_data.get('phone_numbers', set())
        phone_str = ",".join(phoneset)
        promo_set = tx_data.get('promotions', set())
        promo_str = ",".join(promo_set)

        items     = tx_data.get('items', [])
        item_count= len(items)
        sum_items = sum(i.get('amount',0.0) for i in items)
        explicit_total = tx_data.get('explicit_total') or 0.0
        total_amt = explicit_total if explicit_total>0 else sum_items
        tx_time   = tx_data.get('transaction_time')

        loyalty_info = tx_data.get('loyalty_info', {})
        balances = loyalty_info.get('balances', [])
        accounts = loyalty_info.get('accounts', [])
        members  = loyalty_info.get('members', [])  # each member might have nested segments or cards

        promo_items = tx_data.get('promo_items', [])
        documents   = tx_data.get('documents', [])
        tenders     = tx_data.get('tenders', [])

        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            # Upsert generic_transactions
            cur.execute("""
            INSERT INTO generic_transactions (
                trans_id, card_id, first_name, last_name,
                phone_numbers, promotions, item_count, total_amount,
                transaction_time
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trans_id) DO UPDATE SET
                card_id=excluded.card_id,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                phone_numbers=excluded.phone_numbers,
                promotions=excluded.promotions,
                item_count=excluded.item_count,
                total_amount=excluded.total_amount,
                transaction_time=excluded.transaction_time
            """, (
                trans_id, card_id, firstn, lastn, phone_str,
                promo_str, item_count, total_amt, tx_time
            ))
            conn.commit()

            # Insert items
            for it in items:
                cur.execute("""
                INSERT INTO generic_items (
                    trans_id, plu, name, dep_code, quantity, price, amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    trans_id,
                    it.get('plu',''),
                    it.get('name',''),
                    it.get('depCode',''),
                    it.get('qty',0.0),
                    it.get('price',0.0),
                    it.get('amount',0.0)
                ))
            conn.commit()

            # Insert documents
            for doc in documents:
                cur.execute("""
                INSERT INTO generic_documents (
                    trans_id, document_type, barcode, confirmation_level,
                    promotion_id, description
                ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    trans_id,
                    doc.get('documentType'),
                    doc.get('barcode'),
                    doc.get('confirmationLevel'),
                    doc.get('promotionId'),
                    doc.get('description')
                ))
            conn.commit()

            # Insert tenders
            for t in tenders:
                cur.execute("""
                INSERT INTO generic_tenders (
                    trans_id, tender_no, amount, tender_type
                ) VALUES (?, ?, ?, ?)
                """, (
                    trans_id,
                    t.get('tenderNo'),
                    t.get('amount', 0.0),
                    t.get('tenderType','')
                ))
            conn.commit()

            # Insert promotions
            for pm_id in promo_set:
                cur.execute("""
                INSERT INTO generic_promotions (
                    trans_id, promotion_id
                ) VALUES (?, ?)
                """, (trans_id, pm_id))
            conn.commit()

            # Insert promo_items correlation
            for pm in promo_items:
                cur.execute("""
                INSERT INTO generic_promo_items (
                    trans_id, promotion_id, item_id, department_id,
                    allocated_qty, triggered_qty, is_lottery, redeemed_qty
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trans_id,
                    pm.get('promotion_id',''),
                    pm.get('item_id',''),
                    pm.get('department_id',''),
                    pm.get('allocated_qty',0.0),
                    pm.get('triggered_qty',0.0),
                    pm.get('is_lottery',''),
                    pm.get('redeemed_qty',0.0)
                ))
            conn.commit()

            # Insert loyalty balances
            for b in balances:
                cur.execute("""
                INSERT INTO generic_loyalty_balances (
                    trans_id, balance_type, balance_id, name,
                    open_balance, earnings, redemptions, current_balance
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trans_id,
                    b.get('type'),
                    b.get('balance_id'),
                    b.get('name'),
                    b.get('open_balance'),
                    b.get('earnings'),
                    b.get('redemptions'),
                    b.get('current_balance')
                ))

            # Insert loyalty accounts
            for a in accounts:
                cur.execute("""
                INSERT INTO generic_loyalty_accounts (
                    trans_id, acc_id, value, up_to_date
                ) VALUES (?, ?, ?, ?)
                """, (
                    trans_id,
                    a.get('acc_id'),
                    a.get('value'),
                    a.get('up_to_date')
                ))
            conn.commit()

            # Insert loyalty members
            # For each <member>, we might also have nested segments, cards, stores, etc.
            for m in members:
                cur.execute("""
                INSERT INTO generic_loyalty_members (
                    trans_id, last_name, first_name, status, member_external_id
                ) VALUES (?, ?, ?, ?, ?)
                """, (
                    trans_id,
                    m.get('last_name'),
                    m.get('first_name'),
                    m.get('status'),
                    m.get('member_external_id')
                ))
                member_row_id = cur.lastrowid  # so we can link segments or cards if needed

                # If your aggregator is storing segments under m['segments'], do:
                segs = m.get('segments', [])
                for s in segs:
                    cur.execute("""
                    INSERT INTO generic_loyalty_segments (
                        trans_id, member_row_id, segment_id, segment_name
                    ) VALUES (?, ?, ?, ?)
                    """, (
                        trans_id,
                        member_row_id,
                        s.get('segment_id'),
                        s.get('segment_name','')
                    ))

                # If aggregator has "cards" under m['cards']:
                cards = m.get('cards', [])
                for cdat in cards:
                    cur.execute("""
                    INSERT INTO generic_loyalty_member_cards (
                        trans_id, member_row_id, card_id,
                        card_status, expiration_date
                    ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        trans_id,
                        member_row_id,
                        cdat.get('card_id'),
                        cdat.get('card_status'),
                        cdat.get('expiration_date')
                    ))

                # If aggregator has "stores" under m['stores']:
                stores = m.get('stores', [])
                for st in stores:
                    cur.execute("""
                    INSERT INTO generic_loyalty_member_stores (
                        trans_id, member_row_id, store_id
                    ) VALUES (?, ?, ?)
                    """, (
                        trans_id,
                        member_row_id,
                        st.get('store_id')
                    ))

            conn.commit()

        except sqlite3.Error as e:
            self.logger.error(f"SQLite error upserting transaction {trans_id}: {e}")
        finally:
            conn.close()

    def store_metadata(self, key, value):
        """
        Insert or overwrite a key/value in the 'metadata' table.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""
            INSERT INTO metadata (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in store_metadata: {e}")
        finally:
            conn.close()

    def get_all_timestamps(self, table_name="generic_logs", start_ts=None, end_ts=None):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            base_sql = f"SELECT combined_ts FROM {table_name} WHERE combined_ts IS NOT NULL"
            params = []
            if start_ts is not None:
                base_sql += " AND combined_ts >= ?"
                params.append(start_ts)
            if end_ts is not None:
                base_sql += " AND combined_ts <= ?"
                params.append(end_ts)

            base_sql += " ORDER BY combined_ts"
            cur.execute(base_sql, params)
            rows = cur.fetchall()
            timestamps = [float(r[0]) for r in rows if r[0] is not None]
            conn.close()
            return timestamps
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_all_timestamps: {e}")
            return []

    def get_cursor(self):
        """
        Returns a new cursor for advanced queries. 
        Caller must close the connection if you keep a reference to it.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            return conn.cursor()
        except sqlite3.Error as e:
            self.logger.error(f"Could not create DB cursor: {e}")
            return None

    def get_columns(self, table_name="generic_logs"):
        """
        Return a list of column names for the given table.
        """
        cols = []
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table_name})")
            rows = cur.fetchall()
            for r in rows:
                cols.append(r[1])
            conn.close()
        except sqlite3.Error as e:
            self.logger.error(f"SQLite error in get_columns({table_name}): {e}")
        return cols
