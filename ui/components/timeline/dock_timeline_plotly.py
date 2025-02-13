import os
import hashlib
import sqlite3
from datetime import datetime, timedelta
import logging
from PyQt5.QtWidgets import (
    QDockWidget, QWidget, QVBoxLayout, QHBoxLayout, QMessageBox,
    QSizePolicy, QLabel, QComboBox, QDateTimeEdit, QPushButton
)
from PyQt5.QtCore import pyqtSignal, Qt, QObject, QUrl, pyqtSlot, QDateTime
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWebChannel import QWebChannel

import plotly.graph_objects as go
import plotly.io as pio
from dateutil import parser as date_parser

class JSBridge(QObject):
    """
    A simple QObject to bridge JavaScript events back to Python.
    Emits a signal with the clicked datetime string.
    """
    timeClicked = pyqtSignal(str)

    @pyqtSlot(str)
    def onPlotlyClick(self, x_value_str):
        self.timeClicked.emit(x_value_str)

class TimelineDock(QDockWidget):
    """
    A dock that displays a Plotly-based timeline of log events.
    It unifies timestamps (or events) from multiple log sources and shows each source as its own trace.
    This version now computes different shades for similar source types (e.g. IIS, EVTX, GenericLog) 
    and passes along additional row information to be displayed on hover.
    """
    jumpToTimeSignal = pyqtSignal(object)  # Emitted when user clicks the timeline (datetime object)

    def __init__(self, parent=None):
        # (The self.source_colors dict is no longer used for constant colors.)
        self.source_colors = {
            'IIS': 'green',
            'evtx': 'red',
            # Add more base mappings if needed.
        }
        super().__init__("Timeline (Plotly)", parent)
        self.setObjectName("TimelineDock")
        self.logger = logging.getLogger('TimelineDock')
        self.logger.setLevel(logging.DEBUG)
        self.setAllowedAreas(Qt.BottomDockWidgetArea | Qt.TopDockWidgetArea)

        # Dictionaries to hold events.
        # self.timestamp_dict: the (possibly filtered) events for each source.
        # Each event can be a timestamp (float) or a tuple/dict like (timestamp, info).
        # self.original_timestamp_dict: the original, full list for each source.
        self.timestamp_dict = {}
        self.original_timestamp_dict = {}

        # Main container and layout.
        container = QWidget()
        main_layout = QVBoxLayout(container)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # --- Control Panel ---
        self.control_panel = QWidget()
        cp_layout = QHBoxLayout(self.control_panel)
        cp_layout.setContentsMargins(5, 5, 5, 5)
        cp_layout.setSpacing(10)

        cp_layout.addWidget(QLabel("Source:"))
        self.source_combo = QComboBox()
        cp_layout.addWidget(self.source_combo)

        cp_layout.addWidget(QLabel("Start:"))
        self.source_start_edit = QDateTimeEdit()
        self.source_start_edit.setCalendarPopup(True)
        self.source_start_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        cp_layout.addWidget(self.source_start_edit)

        cp_layout.addWidget(QLabel("End:"))
        self.source_end_edit = QDateTimeEdit()
        self.source_end_edit.setCalendarPopup(True)
        self.source_end_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        cp_layout.addWidget(self.source_end_edit)

        self.set_span_btn = QPushButton("Set Source Span")
        self.set_span_btn.clicked.connect(self.setSourceSpan)
        cp_layout.addWidget(self.set_span_btn)

        # New: Reset button to restore original (full) span.
        self.reset_span_btn = QPushButton("Reset Source Span")
        self.reset_span_btn.clicked.connect(self.resetSourceSpan)
        cp_layout.addWidget(self.reset_span_btn)

        self.remove_source_btn = QPushButton("Remove Source")
        self.remove_source_btn.clicked.connect(self.removeSourceTimestamps)
        cp_layout.addWidget(self.remove_source_btn)

        # Add a spacer to push controls to the left.
        cp_layout.addStretch()

        main_layout.addWidget(self.control_panel)

        # --- WebEngineView for Plotly graph ---
        self.web_view = QWebEngineView()
        self.web_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addWidget(self.web_view)

        self.setWidget(container)

        # Setup QWebChannel for JS-to-Python communication.
        self.channel = QWebChannel()
        self.js_bridge = JSBridge()
        self.channel.registerObject("bridge", self.js_bridge)
        self.web_view.page().setWebChannel(self.channel)
        self.js_bridge.timeClicked.connect(self.emitJumpToTimeSignal)
        self.source_combo.currentIndexChanged.connect(self.onSourceChanged)

    def onSourceChanged(self, index):
        """
        When the selected source changes, update the Start/End edits with the min/max
        timestamps for that source (if available).
        """
        source = self.source_combo.itemText(index)
        if source in self.timestamp_dict and self.timestamp_dict[source]:
            # Extract timestamps from events (each event can be a tuple/dict or raw timestamp)
            ts_list = []
            for ev in self.timestamp_dict[source]:
                if isinstance(ev, (tuple, list)):
                    ts_list.append(ev[0])
                elif isinstance(ev, dict):
                    ts_list.append(ev.get("timestamp"))
                else:
                    ts_list.append(ev)
            min_ts = min(ts_list)
            max_ts = max(ts_list)
            from PyQt5.QtCore import QDateTime
            start_qdt = QDateTime.fromSecsSinceEpoch(int(min_ts))
            end_qdt = QDateTime.fromSecsSinceEpoch(int(max_ts))
            self.source_start_edit.setDateTime(start_qdt)
            self.source_end_edit.setDateTime(end_qdt)
        else:
            pass

    def refreshSourceCombo(self):
        """Update the source combo box to reflect the keys in timestamp_dict."""
        current_source = self.source_combo.currentText()
        self.source_combo.clear()
        for src in self.timestamp_dict.keys():
            self.source_combo.addItem(src)
        # Restore previously selected source if possible.
        index = self.source_combo.findText(current_source)
        if index >= 0:
            self.source_combo.setCurrentIndex(index)

    def addTimestamps(self, source_name: str, events: list):
        """
        Called by various docks to add their events.
        Each event can be just a timestamp (float) or a tuple/dict that includes extra info.
        """
        if not events:
            self.logger.warning(f"No events provided by '{source_name}'.")
            return
        # Store a copy in both the current (possibly filtered) and backup dictionaries.
        self.timestamp_dict[source_name] = events[:]
        self.original_timestamp_dict[source_name] = events[:]
        self.logger.debug(f"Added {len(events)} events from '{source_name}'.")
        self.refreshSourceCombo()
        self.updateTimelineUnified()

    def removeTimestamps(self, source_name: str):
        """
        Remove events for a specific source.
        """
        if source_name in self.timestamp_dict:
            del self.timestamp_dict[source_name]
        if source_name in self.original_timestamp_dict:
            del self.original_timestamp_dict[source_name]
        self.logger.debug(f"Removed events for source '{source_name}'.")
        self.refreshSourceCombo()
        self.updateTimelineUnified()

    @pyqtSlot()
    def removeSourceTimestamps(self):
        """
        Called when the user clicks the "Remove Source" button.
        Permanently removes the events for the selected source.
        """
        source = self.source_combo.currentText()
        if source:
            self.removeTimestamps(source)

    @pyqtSlot()
    def setSourceSpan(self):
        """
        Called when the user clicks "Set Source Span."
        Filters the events for the selected source so that only those between the specified start and end
        are retained. Uses the original full list to allow reverting back.
        """
        source = self.source_combo.currentText()
        if not source:
            QMessageBox.warning(self, "No Source Selected", "Please select a source.")
            return

        start_dt = self.source_start_edit.dateTime().toPyDateTime()
        end_dt = self.source_end_edit.dateTime().toPyDateTime()
        if start_dt >= end_dt:
            QMessageBox.warning(self, "Invalid Time Range", "Start time must be before end time.")
            return

        # Filter from the backup list (original events)
        original_events = self.original_timestamp_dict.get(source, [])
        new_events = []
        for ev in original_events:
            # Extract timestamp whether the event is raw or includes extra info
            if isinstance(ev, (tuple, list)):
                ts = ev[0]
            elif isinstance(ev, dict):
                ts = ev.get("timestamp")
            else:
                ts = ev
            if start_dt.timestamp() <= ts <= end_dt.timestamp():
                new_events.append(ev)
        self.timestamp_dict[source] = new_events
        self.logger.debug(f"Source '{source}' span set to {len(new_events)} events (filtered from {len(original_events)}).")
        self.updateTimelineUnified()

    @pyqtSlot()
    def resetSourceSpan(self):
        """
        Resets the selected source's events to the original full set.
        """
        source = self.source_combo.currentText()
        if not source:
            QMessageBox.warning(self, "No Source Selected", "Please select a source.")
            return
        if source in self.original_timestamp_dict:
            self.timestamp_dict[source] = self.original_timestamp_dict[source][:]
            self.logger.debug(f"Source '{source}' span reset to full range ({len(self.timestamp_dict[source])} events).")
            self.updateTimelineUnified()
            # Also update the Start/End edits to reflect the full range.
            ts_list = []
            for ev in self.timestamp_dict[source]:
                if isinstance(ev, (tuple, list)):
                    ts_list.append(ev[0])
                elif isinstance(ev, dict):
                    ts_list.append(ev.get("timestamp"))
                else:
                    ts_list.append(ev)
            if ts_list:
                start_qdt = QDateTime.fromSecsSinceEpoch(int(min(ts_list)))
                end_qdt = QDateTime.fromSecsSinceEpoch(int(max(ts_list)))
                self.source_start_edit.setDateTime(start_qdt)
                self.source_end_edit.setDateTime(end_qdt)
        else:
            QMessageBox.warning(self, "No Backup Found", "No original events available to reset.")

    def green_shade_for_source(self, source: str) -> str:

        h = hashlib.md5(source.encode('utf-8')).hexdigest()
        # Use first 2 hex characters (0-255)
        hash_val = int(h[:2], 16)
        # Map to a range, for example, 50 to 255 (adjust these values as needed)
        green_val = int(50 + (hash_val / 255.0) * (255 - 50))
        return f"rgb(0, {green_val}, 0)"

    def red_shade_for_source(self, source: str) -> str:

        h = hashlib.md5(source.encode('utf-8')).hexdigest()
        hash_val = int(h[:2], 16)
        red_val = int(50 + (hash_val / 255.0) * (255 - 50))
        return f"rgb({red_val}, 0, 0)"

    def yellow_shade_for_source(self, source: str) -> str:

        h = hashlib.md5(source.encode('utf-8')).hexdigest()
        hash_val = int(h[:2], 16)
        yellow_val = int(50 + (hash_val / 255.0) * (255 - 10))
        return f"rgb({yellow_val + 40},{yellow_val}, 50)"

    def updateTimelineUnified(self):
        """
        Merges the events from all sources and displays them in a single Plotly chart.
        Each source is displayed as its own trace with a color based on its type—
        but with a unique shade computed from the source's identifier.
        Additionally, extra event details (if provided) are aggregated and shown on hover.
        """
        if not self.timestamp_dict:
            self.logger.info("No events to display. Showing empty timeline.")
            self.displayEmptyTimeline()
            return

        fig = go.Figure()
        for src, events in self.timestamp_dict.items():
            if not events:
                continue
            # Modified histogramData now returns counts, bins, and aggregated details per bin.
            counts, bins, bin_info = self.histogramData(events)
            # Convert bin start timestamps to datetime objects (skip the last bin edge)
            bins_datetime = [datetime.fromtimestamp(ts) for ts in bins[:-1]]
            # Choose a color based on source type (with unique shading)
            if 'IIS' in src.upper():
                color = self.green_shade_for_source(src)
            elif 'EVTX' in src.upper():
                color = self.red_shade_for_source(src)
            elif src.startswith("GenericLog:"):
                color = self.yellow_shade_for_source(src)
            else:
                color = 'blue'
            # Create custom hover text for each bin
            hover_text = []
            for cnt, info_list in zip(counts, bin_info):
                text = f"Count: {cnt}"
                if info_list:
                    details = "<br>".join(info_list)
                    text += f"<br>Details: {details}"
                hover_text.append(text)

            fig.add_trace(go.Scatter(
                x=bins_datetime,
                y=counts,
                mode='markers',
                marker=dict(size=8, color=color, opacity=0.8),
                name=src,
                text=hover_text,
                hoverinfo='text'
            ))
        fig.update_layout(
            title="Consolidated Log Timeline",
            xaxis=dict(title='Time'),
            yaxis=dict(title='Number of Events'),
            hovermode='closest',
            margin=dict(l=50, r=50, t=50, b=50),
            template='plotly_dark'
        )
        plot_html = pio.to_html(fig, include_plotlyjs=False, full_html=False, div_id='plotly-div')
        self.loadPlotHtml(plot_html)

    def loadPlotHtml(self, plot_html: str):
        """
        Loads the given Plotly HTML into the timeline view.
        """
        html_file_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__),
            '..', '..', '..',
            'utilities', 'html',
            'timeline.html'
        ))
        if not os.path.exists(html_file_path):
            self.logger.error(f"Timeline HTML file not found at: {html_file_path}")
            QMessageBox.critical(self, "File Error", f"Timeline HTML file not found:\n{html_file_path}")
            return

        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_template = f.read()

        final_html = html_template.replace('{{plot}}', plot_html)
        self.web_view.setHtml(
            final_html,
            baseUrl=QUrl.fromLocalFile(os.path.abspath(os.path.dirname(html_file_path)) + '/')
        )
        self.logger.info("Timeline updated with new data.")

    def histogramData(self, events):
        """
        Creates a histogram (with 1-minute resolution) from a list of events.
        Each event can be:
          - a raw timestamp (float), or
          - a tuple/list: (timestamp, info), or
          - a dict with keys 'timestamp' and optionally 'info'.
        Returns counts, bins, and a list of aggregated info (one per bin).
        """
        if not events:
            return [], [], []
        # Process events so that each becomes a tuple (timestamp, info)
        processed = []
        for ev in events:
            if isinstance(ev, (tuple, list)):
                processed.append(ev)
            elif isinstance(ev, dict):
                ts = ev.get('timestamp')
                info = ev.get('info', '')
                processed.append((ts, info))
            else:
                processed.append((ev, ""))
        timestamps = [item[0] for item in processed]
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        delta = max_ts - min_ts
        minutes = int(delta // 60) + 1
        bins = [min_ts + i * 60 for i in range(minutes + 1)]
        counts = [0] * minutes
        bin_info = [[] for _ in range(minutes)]
        for ts, info in processed:
            idx = int((ts - min_ts) // 60)
            if 0 <= idx < minutes:
                counts[idx] += 1
                if info:
                    bin_info[idx].append(info)
        return counts, bins, bin_info

    def displayEmptyTimeline(self):
        """
        Displays an empty timeline with a "No data available" annotation.
        """
        fig = go.Figure(
            data=[],
            layout=go.Layout(
                title="Log Timeline",
                xaxis=dict(title="Time"),
                yaxis=dict(title="Number of Events"),
                annotations=[{
                    "text": "No data available.",
                    "xref": "paper", "yref": "paper",
                    "showarrow": False,
                    "font": {"size": 20}
                }]
            )
        )
        plot_html = pio.to_html(fig, include_plotlyjs=False, full_html=False)
        self.loadPlotHtml(plot_html)

    def displayErrorTimeline(self, message):
        """
        Displays an error message within the timeline view.
        """
        fig = go.Figure(
            data=[],
            layout=go.Layout(
                title="Log Timeline - Error",
                xaxis=dict(title="Time"),
                yaxis=dict(title="Number of Events"),
                annotations=[{
                    "text": f"Error: {message}",
                    "xref": "paper", "yref": "paper",
                    "showarrow": False,
                    "font": {"size": 16, "color": "red"}
                }]
            )
        )
        plot_html = pio.to_html(fig, include_plotlyjs=False, full_html=False)
        self.loadPlotHtml(plot_html)

    @pyqtSlot(str)
    def emitJumpToTimeSignal(self, x_str):
        """
        Parses the clicked x-value from the timeline and emits jumpToTimeSignal.
        """
        try:
            dt = date_parser.parse(x_str)
            self.logger.debug(f"Emitting jumpToTimeSignal with datetime: {dt}")
            self.jumpToTimeSignal.emit(dt)
        except Exception as e:
            self.logger.error(f"Could not parse datetime from '{x_str}': {e}")
            self.displayErrorTimeline(f"Could not parse datetime from '{x_str}': {e}")
