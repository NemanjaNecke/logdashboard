# ui/components/display_logs/IIS/column_selection_dialog.py

from PyQt5.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QCheckBox, QLabel, QScrollArea, QWidget # pylint: disable=no-name-in-module

class ColumnSelectionDialog(QDialog):
    """
    Dialog that allows users to select which columns to display.
    """
    def __init__(self, available_columns, selected_columns=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Columns to Display")
        self.resize(400, 500)
        self.selected_columns = selected_columns if selected_columns else []

        layout = QVBoxLayout(self)

        label = QLabel("Select the columns you want to display:")
        layout.addWidget(label)

        # Scroll area for checkboxes
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        self.checkboxes = []
        for col in available_columns:
            cb = QCheckBox(col.replace('_', ' ').title())
            cb.setChecked(col in self.selected_columns)
            cb.column_name = col  # Store the actual column name
            self.checkboxes.append(cb)
            scroll_layout.addWidget(cb)

        scroll.setWidget(scroll_content)
        layout.addWidget(scroll)

        # Buttons
        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("OK")
        self.cancel_button = QPushButton("Cancel")
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

        # Connect signals
        self.ok_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)

    def get_selected_columns(self):
        """
        Returns the list of selected column names.
        """
        selected = [cb.column_name for cb in self.checkboxes if cb.isChecked()]
        return selected
