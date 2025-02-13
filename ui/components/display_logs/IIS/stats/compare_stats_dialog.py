# ui/components/display_logs/IIS/compare_stats_dialog.py

import logging
from PyQt5.QtCore import Qt  # pylint: disable=no-name-in-module
from PyQt5.QtWidgets import (  # pylint: disable=no-name-in-module
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QTableView,
    QMessageBox,
    QHeaderView,
)
from PyQt5.QtGui import QStandardItemModel, QStandardItem, QColor  # pylint: disable=no-name-in-module


class CompareStatsDialog(QDialog):
    """
    Dialog to compare two tabular reports. Displays the comparison side by side
    and highlights differences between metrics.
    """

    def __init__(self, report1, report2, parent=None):
        """
        Initializes the comparison dialog with two reports.

        :param report1: Dictionary containing 'name' and 'data' for the first report.
        :param report2: Dictionary containing 'name' and 'data' for the second report.
        :param parent: Parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle(f"Compare: {report1['name']} vs {report2['name']}")
        self.resize(1200, 800)

        # Initialize logger
        self.logger = logging.getLogger('CompareStatsDialog')  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member

        # Main layout
        layout = QVBoxLayout(self)

        # Horizontal layout to hold both reports
        h_layout = QHBoxLayout()
        layout.addLayout(h_layout)

        # Report 1
        self.report1_view = QTableView()
        self.report1_model = self.createComparisonModel(report1['data'])
        self.report1_view.setModel(self.report1_model)
        self.report1_view.setWordWrap(True)
        self.report1_view.setTextElideMode(Qt.ElideNone)
        self.report1_view.horizontalHeader().setStretchLastSection(True)
        self.report1_view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.report1_view.setAlternatingRowColors(True)
        self.report1_view.setSortingEnabled(True)
        self.report1_view.resizeColumnsToContents()
        h_layout.addWidget(self.report1_view)

        # Report 2
        self.report2_view = QTableView()
        self.report2_model = self.createComparisonModel(report2['data'])
        self.report2_view.setModel(self.report2_model)
        self.report2_view.setWordWrap(True)
        self.report2_view.setTextElideMode(Qt.ElideNone)
        self.report2_view.horizontalHeader().setStretchLastSection(True)
        self.report2_view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.report2_view.setAlternatingRowColors(True)
        self.report2_view.setSortingEnabled(True)
        self.report2_view.resizeColumnsToContents()
        h_layout.addWidget(self.report2_view)

        # Highlight differences between reports
        self.highlightDifferences()

        # Optional: Add more comparison features here, such as summary statistics or visual charts

    def createComparisonModel(self, df):
        """
        Creates a QStandardItemModel from a pandas DataFrame.

        :param df: pandas DataFrame containing the report data.
        :return: QStandardItemModel populated with the DataFrame data.
        """
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(df.columns.tolist())

        for row in df.itertuples(index=False):
            items = [QStandardItem(str(field)) for field in row]
            for item in items:
                item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            model.appendRow(items)

        return model

    def highlightDifferences(self):
        """
        Highlights the differences between the two reports based on metrics.
        Assumes the first column is 'Metric' and the second is 'Value'.
        """
        # Retrieve metrics and values from Report 1
        metrics1 = {}
        for row in range(self.report1_model.rowCount()):
            metric = self.report1_model.item(row, 0).text()
            value = self.report1_model.item(row, 1).text()
            metrics1[metric] = value

        # Retrieve metrics and values from Report 2
        metrics2 = {}
        for row in range(self.report2_model.rowCount()):
            metric = self.report2_model.item(row, 0).text()
            value = self.report2_model.item(row, 1).text()
            metrics2[metric] = value

        # Find common metrics
        common_metrics = set(metrics1.keys()).intersection(set(metrics2.keys()))
        self.logger.debug(f"Common metrics for comparison: {common_metrics}")

        if not common_metrics:
            self.logger.warning("No common metrics found to compare.")
            QMessageBox.warning(self, "No Common Metrics", "There are no common metrics to compare between the selected reports.")
            return

        # Compare values for common metrics
        differences = []
        for metric in common_metrics:
            value1 = metrics1[metric]
            value2 = metrics2[metric]
            if value1 != value2:
                differences.append(metric)
                # Highlight in Report 1
                for row in range(self.report1_model.rowCount()):
                    if self.report1_model.item(row, 0).text() == metric:
                        self.report1_model.item(row, 1).setBackground(QColor('yellow'))
                        break
                # Highlight in Report 2
                for row in range(self.report2_model.rowCount()):
                    if self.report2_model.item(row, 0).text() == metric:
                        self.report2_model.item(row, 1).setBackground(QColor('yellow'))
                        break

        # Notify the user about the differences
        if differences:
            QMessageBox.information(self, "Differences Found",
                                    f"{len(differences)} differences were highlighted in the comparison.")
        else:
            QMessageBox.information(self, "No Differences",
                                    "No differences were found between the selected reports.")
