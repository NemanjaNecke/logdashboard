import logging
import sqlite3

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QTreeWidget, QTreeWidgetItem,
    QPushButton, QPlainTextEdit, QMessageBox
)

class AnalyticsGadget(QWidget):
    """
    Shows all data stored by the DB manager:
      - Items (generic_items)
      - Unique transactions
      - aggregator snippet
      - Full loyalty info: balances, accounts, members, segments, cards, etc.
    """
    def __init__(self, db_manager=None, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.logger = logging.getLogger("AnalyticsGadget")
        self.logger.setLevel(logging.DEBUG)

        main_layout = QVBoxLayout()
        self.setLayout(main_layout)

        # 1) Items
        self.items_label = QLabel("Items:")
        main_layout.addWidget(self.items_label)

        self.items_tree = QTreeWidget()
        self.items_tree.setHeaderLabels(["TxID", "PLU", "Name", "Dep", "Qty", "Price", "Amount"])
        main_layout.addWidget(self.items_tree)

        # 2) Unique TX
        self.unique_tx_label = QLabel("Unique Transactions:")
        main_layout.addWidget(self.unique_tx_label)

        self.unique_tx_tree = QTreeWidget()
        self.unique_tx_tree.setHeaderLabels(["Transaction ID"])
        main_layout.addWidget(self.unique_tx_tree)

        # 3) "Dark theme" text viewer for aggregator snippet + loyalty details
        self.xml_label = QLabel("Aggregator / Loyalty Data:")
        main_layout.addWidget(self.xml_label)

        self.xml_viewer = QPlainTextEdit()
        self.xml_viewer.setReadOnly(True)
        self.xml_viewer.setStyleSheet("""
            QPlainTextEdit {
                background-color: #2b2b2b;
                color: #f8f8f2;
                font-family: Consolas, "Courier New", monospace;
                font-size: 12px;
                border: 1px solid #444;
            }
        """)
        main_layout.addWidget(self.xml_viewer)

        # Refresh button
        self.refresh_btn = QPushButton("Refresh Analytics")
        self.refresh_btn.clicked.connect(self.loadAnalytics)
        main_layout.addWidget(self.refresh_btn)

        if self.db_manager:
            self.loadAnalytics()

    def clear(self):
        """Clear all UI elements."""
        self.items_tree.clear()
        self.unique_tx_tree.clear()
        self.xml_viewer.clear()

    def loadAnalytics(self):
        """
        Loads and displays ALL data from the DB:
          - items
          - transactions
          - loyalty balances, accounts, members, segments, cards, stores
        """
        self.logger.info("Loading analytics data.")
        self.clear()

        if not self.db_manager:
            self.logger.warning("No DB manager set. Cannot load analytics.")
            return

        cursor = self.db_manager.get_cursor()
        if not cursor:
            QMessageBox.warning(self, "DB Error", "Unable to retrieve DB cursor.")
            return

        try:
            # 1) Items in generic_items
            cursor.execute("""
                SELECT trans_id, plu, name, dep_code, quantity, price, amount
                FROM generic_items
                ORDER BY trans_id
            """)
            rows = cursor.fetchall()
            for row in rows:
                trans_id, plu, name, dep, qty, price, amount = row
                item_node = QTreeWidgetItem([
                    str(trans_id), str(plu), str(name),
                    str(dep), str(qty), str(price), str(amount)
                ])
                self.items_tree.addTopLevelItem(item_node)

            # 2) Unique transactions
            cursor.execute("SELECT trans_id FROM generic_transactions ORDER BY trans_id")
            tx_rows = cursor.fetchall()
            for (txid,) in tx_rows:
                self.unique_tx_tree.addTopLevelItem(QTreeWidgetItem([txid]))

            # 3) aggregator snippet: show first 5 from generic_transactions
            cursor.execute("""
                SELECT trans_id, total_amount, promotions, card_id, phone_numbers
                FROM generic_transactions
                LIMIT 5
            """)
            snippet_rows = cursor.fetchall()
            aggregator_text = ""
            for (txid, amt, promos, card, phones) in snippet_rows:
                aggregator_text += (
                    f"TransactionID: {txid}\n"
                    f"  Total Amount: {amt}\n"
                    f"  Promotions:   {promos}\n"
                    f"  Card ID:      {card}\n"
                    f"  Phones:       {phones}\n"
                    "----------------------------------------\n"
                )
            self.xml_viewer.setPlainText(aggregator_text)

            # 4) Show all loyalty tables in the text viewer as well:

            # (A) generic_loyalty_balances
            cursor.execute("""
                SELECT trans_id, balance_type, balance_id, name,
                       open_balance, earnings, redemptions, current_balance
                FROM generic_loyalty_balances
                ORDER BY id DESC
                LIMIT 10
            """)
            lb_rows = cursor.fetchall()
            if lb_rows:
                text_block = "\n\n[LOYALTY BALANCES] (showing last 10)\n"
                for row in lb_rows:
                    tid,btype,bid,bname,opn,earn,red,cur = row
                    text_block += (f"Tx={tid} Type={btype}, ID={bid}, Name={bname}, "
                                   f"Open={opn}, Earn={earn}, Redeem={red}, Cur={cur}\n")
                self.xml_viewer.appendPlainText(text_block)

            # (B) generic_loyalty_accounts
            cursor.execute("""
                SELECT trans_id, acc_id, value, up_to_date
                FROM generic_loyalty_accounts
                ORDER BY id DESC
                LIMIT 10
            """)
            la_rows = cursor.fetchall()
            if la_rows:
                text_block = "\n\n[LOYALTY ACCOUNTS] (last 10)\n"
                for row in la_rows:
                    tid,accid,val,utd = row
                    text_block += (f"Tx={tid} AccID={accid}, Value={val}, UpToDate={utd}\n")
                self.xml_viewer.appendPlainText(text_block)

            # (C) generic_loyalty_members
            cursor.execute("""
                SELECT id, trans_id, last_name, first_name, status, member_external_id
                FROM generic_loyalty_members
                ORDER BY id DESC
                LIMIT 10
            """)
            mem_rows = cursor.fetchall()
            if mem_rows:
                text_block = "\n\n[LOYALTY MEMBERS] (last 10)\n"
                for row in mem_rows:
                    rowid, tid, ln, fn, st, mid = row
                    text_block += (f"RowID={rowid}, Tx={tid}, LastName={ln}, "
                                   f"FirstName={fn}, Status={st}, ExternalID={mid}\n")
                self.xml_viewer.appendPlainText(text_block)

            # (D) generic_loyalty_segments
            cursor.execute("""
                SELECT id, trans_id, member_row_id, segment_id, segment_name
                FROM generic_loyalty_segments
                ORDER BY id DESC
                LIMIT 10
            """)
            seg_rows = cursor.fetchall()
            if seg_rows:
                text_block = "\n\n[LOYALTY SEGMENTS] (last 10)\n"
                for row in seg_rows:
                    rowid,tid,memid,segid,segname = row
                    text_block += (f"RowID={rowid}, Tx={tid}, MemberRow={memid}, "
                                   f"SegID={segid}, SegName={segname}\n")
                self.xml_viewer.appendPlainText(text_block)

            # (E) generic_loyalty_member_cards
            cursor.execute("""
                SELECT id, trans_id, member_row_id, card_id, card_status, expiration_date
                FROM generic_loyalty_member_cards
                ORDER BY id DESC
                LIMIT 10
            """)
            card_rows = cursor.fetchall()
            if card_rows:
                text_block = "\n\n[LOYALTY MEMBER CARDS] (last 10)\n"
                for row in card_rows:
                    rowid, tid, memid, cid, cstatus, cexp = row
                    text_block += (f"RowID={rowid}, Tx={tid}, MemberRow={memid}, "
                                   f"CardID={cid}, Status={cstatus}, Exp={cexp}\n")
                self.xml_viewer.appendPlainText(text_block)

            # (F) generic_loyalty_member_stores
            cursor.execute("""
                SELECT id, trans_id, member_row_id, store_id
                FROM generic_loyalty_member_stores
                ORDER BY id DESC
                LIMIT 10
            """)
            store_rows = cursor.fetchall()
            if store_rows:
                text_block = "\n\n[LOYALTY MEMBER STORES] (last 10)\n"
                for row in store_rows:
                    rowid, tid, memid, stid = row
                    text_block += (f"RowID={rowid}, Tx={tid}, MemberRow={memid}, StoreID={stid}\n")
                self.xml_viewer.appendPlainText(text_block)

            self.logger.info("Analytics data loaded successfully.")

        except sqlite3.Error as e:
            self.logger.error(f"SQLite error loading analytics: {e}")
            QMessageBox.critical(self, "Analytics Error", f"Failed to load analytics data:\n{e}")
        finally:
            cursor.close()
