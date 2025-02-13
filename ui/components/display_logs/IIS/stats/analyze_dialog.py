# ui/components/IIS/analysis_dialog.py

from PyQt5.QtWidgets import (# pylint: disable=no-name-in-module
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QFileDialog, QMessageBox, QListWidget, QListWidgetItem,
    QStackedLayout, QSpinBox, QCheckBox, QLineEdit
)
from PyQt5.QtCore import Qt, pyqtSignal # pylint: disable=no-name-in-module
import logging

class AnalysisDialog(QDialog):
    """
    Dialog that guides the user through:
      (1) Selecting analysis mode,
      (2) Selecting the appropriate number of files,
      (3) Entering optional analysis parameters,
      (4) Confirming and starting the analysis.

    Emits:
      analysis_selected(file_paths, mode, params)
    """

    # We'll emit: ( [list of file paths], 'single'/'cluster'/'multiple', {params} )
    analysis_selected = pyqtSignal(list, str, dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("IIS Log Analysis")
        self.setModal(True)
        self.resize(600, 500)

        self.logger = logging.getLogger(self.__class__.__name__) # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member

        # State
        self.selected_files = []   # List of chosen file paths
        self.mode = "single"       # 'single', 'cluster', or 'multiple'
        self.analysis_params = {}  # Dictionary of user-defined parameters

        # Main Layout
        main_layout = QVBoxLayout()
        self.setLayout(main_layout)

        # Stacked Layout => 4 steps
        self.stacked_layout = QStackedLayout()
        main_layout.addLayout(self.stacked_layout)

        # Step 1: Pick Mode
        self.step1_widget = self.create_step1()
        self.stacked_layout.addWidget(self.step1_widget)

        # Step 2: Select Files
        self.step2_widget = self.create_step2()
        self.stacked_layout.addWidget(self.step2_widget)

        # Step 3: Analysis Options
        self.step3_widget = self.create_step3_options()
        self.stacked_layout.addWidget(self.step3_widget)

        # Step 4: Confirmation
        self.step4_widget = self.create_step4_confirm()
        self.stacked_layout.addWidget(self.step4_widget)

        # Start on step 1
        self.stacked_layout.setCurrentWidget(self.step1_widget)

    # ------------------------------------------------------------------
    # STEP 1: SELECT MODE
    # ------------------------------------------------------------------
    def create_step1(self):
        widget = QDialog()
        layout = QVBoxLayout(widget)
        widget.setLayout(layout)

        label = QLabel("Select Analysis Mode:")
        layout.addWidget(label)

        self.mode_combo = QComboBox()
        self.mode_combo.addItems([
            "Single File",
            "Cluster Mode (Merge Two Files)",
            "Multiple Files (Same Folder)"
        ])
        layout.addWidget(self.mode_combo)

        button_layout = QHBoxLayout()
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        next_button = QPushButton("Next")
        next_button.clicked.connect(self.go_to_step2)
        button_layout.addStretch()
        button_layout.addWidget(cancel_button)
        button_layout.addWidget(next_button)

        layout.addLayout(button_layout)
        return widget

    def go_to_step2(self):
        """
        User clicked Next on Step 1 => gather mode, go to Step 2.
        """
        mode_text = self.mode_combo.currentText()
        self.logger.debug(f"Selected Mode Text: {mode_text}")

        if mode_text == "Single File":
            self.mode = "single"
        elif mode_text == "Cluster Mode (Merge Two Files)":
            self.mode = "cluster"
        elif mode_text == "Multiple Files (Same Folder)":
            self.mode = "multiple"
        else:
            self.logger.warning(f"Unknown mode selected: {mode_text}")
            self.mode = "single"  # fallback

        # Update the instruction label on step2
        if self.mode == "single":
            self.files_label.setText("Please select exactly ONE IIS log file.")
        elif self.mode == "cluster":
            self.files_label.setText("Please select exactly TWO IIS log files.")
        else:  # multiple
            self.files_label.setText("Please select MULTIPLE IIS log files from the same folder.")

        self.stacked_layout.setCurrentWidget(self.step2_widget)

    # ------------------------------------------------------------------
    # STEP 2: SELECT FILES
    # ------------------------------------------------------------------
    def create_step2(self):
        widget = QDialog()
        layout = QVBoxLayout(widget)
        widget.setLayout(layout)

        # Label (will be updated based on chosen mode)
        self.files_label = QLabel("Select files based on the chosen mode.")
        self.files_label.setWordWrap(True)
        layout.addWidget(self.files_label)

        # List of selected files
        self.files_list = QListWidget()
        layout.addWidget(self.files_list)

        # Buttons
        button_layout = QHBoxLayout()

        back_button = QPushButton("Back")
        back_button.clicked.connect(self.go_to_step1)

        select_button = QPushButton("Select File(s)")
        select_button.clicked.connect(self.select_files)

        next_button = QPushButton("Next")
        next_button.clicked.connect(self.go_to_step3_options)

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(back_button)
        button_layout.addWidget(select_button)
        button_layout.addStretch()
        button_layout.addWidget(next_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        return widget

    def select_files(self):
        """
        Opens a file dialog to pick log files. Enforces the correct # of files for each mode:
          single -> exactly 1
          cluster -> exactly 2
          multiple -> at least 2
        """
        # We'll allow multiple selection in all cases, then enforce below
        dialog_title = "Select IIS Log Files"
        filter_str = "IIS Log Files (*.log *.txt);;All Files (*)"
        file_paths, _ = QFileDialog.getOpenFileNames(self, dialog_title, "", filter_str)

        if not file_paths:
            return  # user canceled

        # Validate based on mode
        if self.mode == "single":
            # force exactly 1
            if len(file_paths) > 1:
                QMessageBox.warning(
                    self,
                    "Selection Error",
                    "You chose more than one file for Single File mode. Only the first will be used."
                )
                self.selected_files = [file_paths[0]]
            else:
                self.selected_files = file_paths
        elif self.mode == "cluster":
            # require exactly 2
            if len(file_paths) != 2:
                QMessageBox.warning(
                    self,
                    "Selection Error",
                    "Cluster mode requires exactly TWO files. Please select exactly two."
                )
                return  # user can try again
            else:
                self.selected_files = file_paths
        else:
            # multiple => at least 2
            if len(file_paths) < 2:
                QMessageBox.warning(
                    self,
                    "Selection Error",
                    "Multiple Files mode requires at least two files."
                )
                return
            else:
                self.selected_files = file_paths

        # Update UI
        self.update_files_list()

    def update_files_list(self):
        """
        Clears and repopulates the QListWidget with self.selected_files.
        """
        self.files_list.clear()
        for f in self.selected_files:
            self.files_list.addItem(QListWidgetItem(f))

    def go_to_step1(self):
        self.stacked_layout.setCurrentWidget(self.step1_widget)

    def go_to_step3_options(self):
        """
        From Step2 => Step3 (Analysis Options).
        First ensure we have the correct # of files for the mode.
        """
        required = 1 if self.mode == 'single' else (2 if self.mode == 'cluster' else 2)
        if len(self.selected_files) < required:
            QMessageBox.warning(
                self,
                "File Count Error",
                f"You must select at least {required} file(s) for this mode."
            )
            return
        self.stacked_layout.setCurrentWidget(self.step3_widget)

    # ------------------------------------------------------------------
    # STEP 3: ANALYSIS OPTIONS
    # ------------------------------------------------------------------
    def create_step3_options(self):
        widget = QDialog()
        layout = QVBoxLayout(widget)

        title_label = QLabel("Analysis Options")
        layout.addWidget(title_label)

        # A spinbox for 'slow request threshold'
        threshold_layout = QHBoxLayout()
        threshold_label = QLabel("Slow Request Threshold (ms):")
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(1, 9999999)
        self.threshold_spin.setValue(5000)  # default 5s
        threshold_layout.addWidget(threshold_label)
        threshold_layout.addWidget(self.threshold_spin)
        layout.addLayout(threshold_layout)

        # Columns to parse
        columns_layout = QHBoxLayout()
        columns_label = QLabel("Columns to Parse (comma-separated):")
        self.columns_edit = QLineEdit()
        self.columns_edit.setText("date,time,cs-method,sc-status,time-taken,cs-uri-stem")
        columns_layout.addWidget(columns_label)
        columns_layout.addWidget(self.columns_edit)
        layout.addLayout(columns_layout)

        # Generate advanced text report?
        self.adv_report_checkbox = QCheckBox("Generate Advanced Text Report?")
        self.adv_report_checkbox.setChecked(True)
        layout.addWidget(self.adv_report_checkbox)

        # Nav buttons
        button_layout = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.go_back_to_step2)
        next_button = QPushButton("Next")
        next_button.clicked.connect(self.go_to_step4_confirm)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(back_button)
        button_layout.addStretch()
        button_layout.addWidget(cancel_button)
        button_layout.addWidget(next_button)

        layout.addLayout(button_layout)
        return widget

    def go_back_to_step2(self):
        self.stacked_layout.setCurrentWidget(self.step2_widget)

    def go_to_step4_confirm(self):
        """
        Saves user input from Step3 into self.analysis_params, then moves to step4.
        """
        self.analysis_params['slow_request_threshold_ms'] = self.threshold_spin.value()
        self.analysis_params['selected_columns'] = [
            col.strip() for col in self.columns_edit.text().split(",")
        ]
        self.analysis_params['generate_advanced_report'] = self.adv_report_checkbox.isChecked()

        self.logger.debug(f"Analysis options: {self.analysis_params}")

        # Move on to final confirmation
        self.stacked_layout.setCurrentWidget(self.step4_widget)
        self.populate_confirmation_list()

    # ------------------------------------------------------------------
    # STEP 4: CONFIRMATION
    # ------------------------------------------------------------------
    def create_step4_confirm(self):
        widget = QDialog()
        layout = QVBoxLayout(widget)

        self.confirm_label = QLabel("Please confirm your selections:")
        layout.addWidget(self.confirm_label)

        self.confirm_list = QListWidget()
        layout.addWidget(self.confirm_list)

        # Buttons
        button_layout = QHBoxLayout()
        back_button = QPushButton("Back")
        back_button.clicked.connect(self.go_back_to_step3_options)
        analyze_button = QPushButton("Start Analysis")
        analyze_button.clicked.connect(self.start_analysis)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(back_button)
        button_layout.addStretch()
        button_layout.addWidget(cancel_button)
        button_layout.addWidget(analyze_button)
        layout.addLayout(button_layout)

        return widget

    def go_back_to_step3_options(self):
        self.stacked_layout.setCurrentWidget(self.step3_widget)

    def populate_confirmation_list(self):
        """
        Show the final summary of selected mode, files, and params.
        """
        self.confirm_list.clear()

        # 1) Show mode
        self.confirm_list.addItem(f"Mode: {self.mode}")
        # 2) Show files
        self.confirm_list.addItem("Selected Files:")
        for fp in self.selected_files:
            self.confirm_list.addItem(f"  - {fp}")
        # 3) Show analysis params
        self.confirm_list.addItem("Parameters:")
        thr = self.analysis_params.get('slow_request_threshold_ms', 5000)
        cols = self.analysis_params.get('selected_columns', [])
        adv = self.analysis_params.get('generate_advanced_report', True)

        self.confirm_list.addItem(f"  Slow Request Threshold (ms): {thr}")
        self.confirm_list.addItem(f"  Selected Columns: {', '.join(cols)}")
        self.confirm_list.addItem(f"  Generate Advanced Report: {adv}")

    def start_analysis(self):
        """
        Final check => emit analysis_selected(files, mode, analysis_params).
        """
        # Double-check we have enough files
        if self.mode == "single":
            required = 1
        elif self.mode == "cluster":
            required = 2
        else:
            required = 2  # multiple

        if len(self.selected_files) < required:
            QMessageBox.warning(
                self,
                "Selection Error",
                f"Please select at least {required} file(s) for '{self.mode}' mode."
            )
            self.logger.warning(f"Not enough files for mode '{self.mode}'.")
            return

        # Everything is validated; emit the final signal
        self.analysis_selected.emit(self.selected_files, self.mode, self.analysis_params)
        self.accept()
