import logging
from PyQt5.QtWidgets import (# pylint: disable=no-name-in-module
    QDialog, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QComboBox, QPushButton
)

class SearchDialog(QDialog):
    """
    A dialog that allows the user to search the IIS logs.

    The user may enter a search term and choose a column from the provided list.
    If "All Columns" is selected, the search will be performed across all available columns.
    
    Attributes:
        term_edit (QLineEdit): Input field for the search string.
        column_combo (QComboBox): Dropdown for selecting a column or "All Columns".
    """
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Search Logs")
        self.resize(400, 150)
        self.logger = logging.getLogger("SearchDialog") # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member

        layout = QVBoxLayout(self)

        # --- Search Term ---
        term_layout = QHBoxLayout()
        term_label = QLabel("Search Term:")
        self.term_edit = QLineEdit()
        term_layout.addWidget(term_label)
        term_layout.addWidget(self.term_edit)
        layout.addLayout(term_layout)

        # --- Column Selection ---
        column_layout = QHBoxLayout()
        column_label = QLabel("Column:")
        self.column_combo = QComboBox()
        self.column_combo.addItem("All Columns")  # Default option
        self.column_combo.addItems(available_columns)
        column_layout.addWidget(column_label)
        column_layout.addWidget(self.column_combo)
        layout.addLayout(column_layout)

        # --- Dialog Buttons ---
        button_layout = QHBoxLayout()
        self.ok_button = QPushButton("Search")
        self.cancel_button = QPushButton("Cancel")
        button_layout.addStretch()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

        # Connect signals
        self.ok_button.clicked.connect(self.accept)
        self.cancel_button.clicked.connect(self.reject)

    def getSearchCriteria(self):
        """
        Retrieves the search criteria entered by the user.

        Returns:
            tuple: A tuple (search_term, column) where search_term is a string and 
                   column is either the name of the selected column or None if "All Columns" is selected.
        """
        term = self.term_edit.text().strip()
        column = self.column_combo.currentText()
        if column == "All Columns":
            column = None
        return term, column
