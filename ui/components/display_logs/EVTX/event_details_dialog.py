# ui/components/display_logs/EVTX/event_details_dialog.py

from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QPushButton
from PyQt5.QtCore import Qt
import json


class EventDetailsDialog(QDialog):
    """
    Shows all available fields in event_data_dict, ignoring raw_xml entirely.
    """

    def __init__(self, event_data_dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Event Details")
        self.setMinimumSize(600, 400)

        layout = QVBoxLayout(self)

        details_str = ""
        for key, val in event_data_dict.items():
            # Skip if key == "raw_xml"
            if key == "raw_xml":
                continue
            details_str += f"{key}: {str(val)}\n"

        # Optional: parse "EventData" as JSON
        event_data_raw = event_data_dict.get("EventData", "")
        try:
            parsed = json.loads(event_data_raw)
            details_str += "\n=== EventData (pretty JSON) ===\n"
            details_str += json.dumps(parsed, indent=4)
        except (json.JSONDecodeError, TypeError):
            details_str += f"\n=== EventData (plain) ===\n{event_data_raw}"

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setPlainText(details_str)

        layout.addWidget(self.text_edit)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignRight)
