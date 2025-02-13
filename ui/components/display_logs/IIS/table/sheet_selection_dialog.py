# ui/components/display_logs/IIS/sheet_selection_dialog.py

from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QCheckBox
)
from PyQt5.QtCore import Qt  # pylint: disable=no-name-in-module

class SheetSelectionDialog(QDialog):
    """
    Dialog to allow users to select which sheets to include in the final Excel report.
    """
    def __init__(self, sheet_names, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Sheets to Save")
        self.resize(400, 300)
        self.selected_sheets = []

        layout = QVBoxLayout()
        self.setLayout(layout)

        label = QLabel("Select the sheets you want to include in the final report:")
        layout.addWidget(label)

        self.sheet_checkboxes = []
        for sheet in sheet_names:
            checkbox = QCheckBox(sheet)
            checkbox.setChecked(True)  # Default to selected
            self.sheet_checkboxes.append(checkbox)
            layout.addWidget(checkbox)

        button_layout = QHBoxLayout()
        self.select_all_button = QPushButton("Select All")
        self.select_all_button.clicked.connect(self.select_all)
        self.deselect_all_button = QPushButton("Deselect All")
        self.deselect_all_button.clicked.connect(self.deselect_all)
        self.ok_button = QPushButton("OK")
        self.ok_button.clicked.connect(self.accept)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.select_all_button)
        button_layout.addWidget(self.deselect_all_button)
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

    def select_all(self):
        """
        Selects all checkboxes.
        """
        for checkbox in self.sheet_checkboxes:
            checkbox.setChecked(True)

    def deselect_all(self):
        """
        Deselects all checkboxes.
        """
        for checkbox in self.sheet_checkboxes:
            checkbox.setChecked(False)

    def get_selected_sheets(self):
        """
        Returns a list of selected sheet names.
        """
        return [cb.text() for cb in self.sheet_checkboxes if cb.isChecked()]
