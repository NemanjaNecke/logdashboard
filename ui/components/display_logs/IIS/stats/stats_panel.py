# ui/components/display_logs/IIS/stats_panel.py

import logging
from PyQt5.QtCore import Qt, pyqtSignal # pylint: disable=no-name-in-module
from PyQt5.QtWidgets import ( # pylint: disable=no-name-in-module
    QTreeWidget, QTreeWidgetItem, QPushButton,
    QVBoxLayout, QWidget, QDialog, QHBoxLayout, QLabel, QSpinBox, QComboBox 
)

class StatsPanel(QWidget):
    """
    A widget that displays field statistics and allows filtering based on selections and thresholds.
    """
    # Signal emitted when a filter is applied from the stats panel
    statsFilterApplied = pyqtSignal(str, object, str)  # column_name, value, operator
    # New signal for refreshing statistics
    refreshStatsRequested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger('StatsPanel') # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member

        # Layout
        main_layout = QVBoxLayout(self)
        self.setLayout(main_layout)

        # QTreeWidget
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["Field / Value", "Count"])
        self.tree_widget.setColumnCount(2)
        self.tree_widget.setColumnWidth(0, 150)
        self.tree_widget.setColumnWidth(1, 80)
        self.tree_widget.itemClicked.connect(self.onItemClicked)
        main_layout.addWidget(self.tree_widget)

        # Controls Layout
        controls_layout = QHBoxLayout()

        # Refresh Statistics Button (Assuming it exists)
        self.refresh_stats_button = QPushButton("Refresh Statistics")
        self.refresh_stats_button.clicked.connect(self.onRefreshStatisticsClicked)
        controls_layout.addWidget(self.refresh_stats_button)
        
        # NEW BUTTON: Set Time Taken Threshold
        self.set_time_threshold_button = QPushButton("Set Time Taken Threshold")
        self.set_time_threshold_button.clicked.connect(self.openTimeThresholdDialog)
        controls_layout.addWidget(self.set_time_threshold_button)

        controls_layout.addStretch()
        main_layout.addLayout(controls_layout)

        # Initialize thresholds and field types
        self.thresholds = {"time_taken": 0}  # Only 'time_taken' has a threshold
        self.threshold_fields = {"time_taken"}
        self.categorical_fields = {"sc_status", "cs_uri_stem", "cs_method", "c_ip"}

        self.current_stats = {}
        self.active_stats_filters = {}
        self.refresh_stats_button.clicked.connect(self.onRefreshStatisticsClicked)

    def onRefreshStatisticsClicked(self):
        """
        Slot triggered when the Refresh Statistics button is clicked.
        Emits a signal to notify that a statistics refresh is requested.
        """
        self.logger.info("Refresh Statistics button clicked. Emitting refreshStatsRequested signal.")
        self.refreshStatsRequested.emit()

    def openTimeThresholdDialog(self):
        """
        Opens a dialog to set the time_taken threshold.
        """
        dialog = QDialog(self)
        dialog.setWindowTitle("Set Time Taken Threshold")
        dialog.resize(300, 150)
        layout = QVBoxLayout(dialog)

        # Instructions
        instructions = QLabel("Set minimum 'time_taken' threshold (in milliseconds):")
        instructions.setWordWrap(True)
        layout.addWidget(instructions)

        # Threshold input
        threshold_layout = QHBoxLayout()
        threshold_label = QLabel("Threshold:")
        self.time_threshold_spin = QSpinBox()
        self.time_threshold_spin.setMinimum(0)
        self.time_threshold_spin.setMaximum(1000000)
        self.time_threshold_spin.setValue(self.thresholds.get("time_taken", 0))
        threshold_layout.addWidget(threshold_label)
        threshold_layout.addWidget(self.time_threshold_spin)
        layout.addLayout(threshold_layout)

        # Buttons
        button_layout = QHBoxLayout()
        ok_button = QPushButton("Apply")
        cancel_button = QPushButton("Cancel")
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)

        # Connect buttons
        ok_button.clicked.connect(dialog.accept)
        cancel_button.clicked.connect(dialog.reject)

        if dialog.exec_() == QDialog.Accepted:
            threshold = self.time_threshold_spin.value()
            self.logger.debug(f"Time Taken Threshold set to: {threshold}")
            self.thresholds["time_taken"] = threshold

            # Emit signal to apply the threshold
            self.statsFilterApplied.emit("time_taken", threshold, "gt")

            # Refresh the stats to reflect the threshold
            self.populateStats(self.current_stats, self.active_stats_filters)

    def populateStats(self, stats_dict, active_filters=None):
        """
        Populates the stats panel (a QTreeWidget) with field-level statistics and adjusts the 
        check states based on active filters. This function creates a tree view where each 
        top-level item represents a field and each child item represents a specific value with 
        its occurrence count.

        The check state of each child item is determined by the current active filters stored 
        in self.active_stats_filters. The intended behavior is as follows:
        - If an item was previously checked (i.e. included in the active data set), clicking it 
            will remove that filter (i.e. unload that data) and mark the item as unchecked.
        - If an item was previously unchecked (i.e. filtered out), clicking it will re-apply the 
            filter (i.e. load that data back) and mark the item as checked.

        The active filter state is maintained in self.active_stats_filters. If the optional 
        parameter `active_filters` is provided, it will update the current active filter state; 
        if it is None, the existing state is preserved.

        In addition, for any field defined as a threshold field (in self.threshold_fields), only 
        values with counts greater than or equal to the threshold (defined in self.thresholds) will 
        be displayed.

        Args:
            stats_dict (dict): A dictionary containing statistics in the format:
                            { "field1": {value1: count1, value2: count2, ...}, ... }.
            active_filters (dict, optional): A dictionary of currently active filters in the format:
                            { "field1": [(operator, value), ...], ... }.
                            If provided, it will update the internal filter state; default is None.

        Returns:
            None
        """
        self.logger.debug("Populating StatsPanel with provided stats dictionary.")
        self.tree_widget.clear()

        if not stats_dict:
            self.logger.debug("No stats found to display.")
            return

        # Update active filter state only if new filters are provided.
        if active_filters is not None:
            self.active_stats_filters = active_filters

        self.current_stats = stats_dict  # Store current stats for threshold application

        for field, value_counts in stats_dict.items():
            root_item = QTreeWidgetItem([field, ""])
            self.tree_widget.addTopLevelItem(root_item)

            # Sort values by highest count descending.
            sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
            for value, count in sorted_values:
                # For threshold fields, skip values that do not meet the threshold.
                if field in self.threshold_fields and count < self.thresholds.get(field, 0):
                    continue

                child_item = QTreeWidgetItem([str(value), str(count)])
                child_item.setFlags(child_item.flags() | Qt.ItemIsUserCheckable)
                
                # Determine check state based on the current active filter state.
                # If this field/value pair is in the active filters, it means it was previously 
                # filtered (removed) so mark it as unchecked. Otherwise, mark it as checked.
                if field in self.active_stats_filters and value in [f[1] for f in self.active_stats_filters[field]]:
                    child_item.setCheckState(0, Qt.Unchecked)
                else:
                    child_item.setCheckState(0, Qt.Checked)
                
                # Store the (field, value) pair in the item's data for later use.
                child_item.setData(0, Qt.UserRole, (field, value))
                root_item.addChild(child_item)

        self.tree_widget.expandAll()
        self.logger.debug("StatsPanel populated successfully.")

    def onItemClicked(self, item):
        """
        Handles clicks on items to apply filters.
        """
        if item.parent() is None:
            # Clicked on the field name (top-level); do nothing
            return

        field, value = item.data(0, Qt.UserRole)
        checked = item.checkState(0) == Qt.Checked

        if field in self.categorical_fields:
            # For categorical fields, emit the filter signal
            self.logger.debug(f"StatsPanel item clicked: {field} = {value} -> {'Checked' if checked else 'Unchecked'}")
            self.statsFilterApplied.emit(field, value, 'eq' if checked else 'neq')
        elif field in self.threshold_fields:
            # For numerical fields, handle threshold filtering
            self.logger.debug(f"StatsPanel numeric item clicked: {field} = {value} -> {'Checked' if checked else 'Unchecked'}")
            if checked:
                # If checked, apply 'greater than' filter
                self.statsFilterApplied.emit(field, self.thresholds.get(field, 0), 'gt')
            else:
                # If unchecked, remove the threshold filter
                self.statsFilterApplied.emit(field, self.thresholds.get(field, 0), 'lt')  # Example logic

    def setThresholds(self, thresholds):
        """
        Sets the threshold values and repopulates the stats.

        :param thresholds: Dictionary {field: threshold, ...}
        """
        self.thresholds.update(thresholds)
        self.logger.debug(f"Thresholds set to: {self.thresholds}")
        # Re-populate stats with new thresholds
        self.populateStats(self.current_stats, self.active_stats_filters)
