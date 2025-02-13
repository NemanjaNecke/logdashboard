# services/logging/dock_log.py

import logging
from PyQt5.QtWidgets import QDockWidget, QTextEdit, QVBoxLayout, QWidget
from PyQt5.QtCore import Qt, QObject, pyqtSignal
from PyQt5.QtGui import QTextCursor  # Added import for QTextCursor


class LogEmitter(QObject):
    """
    A QObject to emit log messages safely to the main thread.
    """
    log_message = pyqtSignal(str)


class LogDock(QDockWidget):
    """
    A dockable widget that displays application logs in a QTextEdit.
    """
    def __init__(self, parent=None):
        super().__init__("Application Log", parent)
        self.setAllowedAreas(Qt.AllDockWidgetAreas)
        self.logger = logging.getLogger('LogDock')
        # Initialize the text edit widget
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)

        # Set up the layout
        layout = QVBoxLayout()
        layout.addWidget(self.text_edit)

        container = QWidget()
        container.setLayout(layout)
        self.setWidget(container)

        # Initialize the LogEmitter
        self.log_emitter = LogEmitter()
        self.log_emitter.log_message.connect(self.append_to_text_edit)

        # Configure logging
        self.setupLogging()

        self.logger = logging.getLogger('LogDock')
        self.logger.debug("LogDock initialized and logging setup completed.")

    def setupLogging(self):
        """
        Sets up a dedicated logger for the LogDock with a custom handler.
        """
        self.logger = logging.getLogger('ApplicationLogger')
        self.logger.setLevel(logging.DEBUG)  # Capture all levels

        # Prevent adding multiple handlers if setupLogging is called multiple times
        if not self.logger.handlers:
            # Create and configure the TextEditHandler
            text_edit_handler = self.TextEditHandler(self.log_emitter)
            text_edit_handler.setLevel(logging.DEBUG)

            # Define a formatter and set it for the handler
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            text_edit_handler.setFormatter(formatter)

            # Add the handler to the logger
            self.logger.addHandler(text_edit_handler)

            self.logger.debug("TextEditHandler added to ApplicationLogger.")

    def appendLog(self, message: str, level=logging.INFO):
        """
        Appends a log message to the QTextEdit with the specified log level.
        """
        if level == logging.DEBUG:
            self.logger.debug(message)
        elif level == logging.INFO:
            self.logger.info(message)
        elif level == logging.WARNING:
            self.logger.warning(message)
        elif level == logging.ERROR:
            self.logger.error(message)
        elif level == logging.CRITICAL:
            self.logger.critical(message)
        else:
            self.logger.info(message)  # Default to INFO

    def append_to_text_edit(self, message: str):
        """
        Slot to append messages to the QTextEdit. Ensures it's executed in the main thread.
        """
        # Optional: Implement color-coding based on log level
        if "DEBUG" in message:
            color = "gray"
        elif "INFO" in message:
            color = "black"
        elif "WARNING" in message:
            color = "orange"
        elif "ERROR" in message:
            color = "red"
        elif "CRITICAL" in message:
            color = "darkred"
        else:
            color = "black"
        
        # Format the message with HTML to apply color
        formatted_message = f'<span style="color:{color}">{message}</span>'
        self.text_edit.append(formatted_message)
        
        # Scroll to the end to show the latest message
        self.text_edit.moveCursor(QTextCursor.End)


    class TextEditHandler(logging.Handler):
        """
        A custom logging handler that emits log messages via LogEmitter.
        """
        def __init__(self, emitter):
            super().__init__()
            self.emitter = emitter

        def emit(self, record):
            try:
                msg = self.format(record)
                self.emitter.log_message.emit(msg)
            except Exception:
                self.handleError(record)
