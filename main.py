# main.py

import sys
import logging

def exception_hook(exc_type, exc_value, exc_traceback):
    logging.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)) #pylint: disable=no-member
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = exception_hook

from PyQt5.QtWidgets import QApplication
from ui.main_window import MainWindow
import qdarkstyle

from services.logging.logging_config import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger('Main')
    logger.info("Starting Log Dashboard application.")
    app = QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())  # Apply dark theme
    window = MainWindow()
    window.show()
    logger.debug("MainWindow displayed.")
    exit_code = app.exec_()
    logger.info(f"Log Dashboard application exited with code: {exit_code}")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()