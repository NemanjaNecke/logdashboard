# services/analyze/iis_analyze_worker.py

import os
from PyQt5.QtCore import QObject, pyqtSignal, QRunnable  # pylint: disable=no-name-in-module
from services.analyze.IIS.iis_analyze import IISLogAnalyzer
import logging
import tempfile
import shutil

class AnalyzerSignals(QObject):
    finished = pyqtSignal(str, str, str)  # Path to the generated Excel, temp_excel_path, temp_dir
    error = pyqtSignal(str)
    progress = pyqtSignal(str)  # New signal for progress updates

class AnalyzerWorker(QRunnable):
    """
    Worker thread for performing IIS log analysis.
    """
    def __init__(self, file_paths, mode, temp_excel_path, temp_dir, analysis_params):
        super().__init__()
        self.file_paths = file_paths
        self.mode = mode  # 'single', 'cluster', 'multiple'
        self.signals = AnalyzerSignals()
        self.logger = logging.getLogger(self.__class__.__name__)  # pylint: disable=no-member
        self.logger.setLevel(logging.DEBUG)  # pylint: disable=no-member
        self.analysis_params = analysis_params
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()  # pylint: disable=no-member
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # pylint: disable=no-member
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Initialize interruption flag
        self._is_interrupted = False
        self.temp_excel_path = temp_excel_path
        self.temp_dir = temp_dir

    def set_interrupted(self):
        """
        Sets the interruption flag to True to signal cancellation.
        """
        self._is_interrupted = True
        self.logger.info("Worker received interruption signal.")

    def run(self):
        try:
            self.logger.info(f"Starting analysis with mode '{self.mode}' and files: {self.file_paths}")

            analyzer = IISLogAnalyzer()
            excel_path = analyzer.analyze_logs(
                self.file_paths,
                mode=self.mode,
                output_path=self.temp_excel_path,
                interruption_flag=lambda: self._is_interrupted,
                progress_callback=self.emit_progress,

                # Let's pass the custom params in a new argument (we'll add it below)
                extra_params=self.analysis_params
            )

            if self._is_interrupted:
                self.logger.info("Analysis was interrupted by the user.")
                self.signals.error.emit("Analysis was canceled by the user.")
                # Clean up temporary directory
                try:
                    shutil.rmtree(self.temp_dir)
                    self.logger.debug(f"Temporary directory '{self.temp_dir}' has been removed due to interruption.")
                except Exception as e:
                    self.logger.warning(f"Failed to remove temporary directory '{self.temp_dir}': {e}")
                return

            if excel_path:
                self.logger.info(f"Analysis completed successfully. Excel report saved at: {excel_path}")
                self.signals.finished.emit(excel_path, self.temp_excel_path, self.temp_dir)
            else:
                error_msg = "Analysis failed. No report was generated."
                self.logger.error(error_msg)
                self.signals.error.emit(error_msg)
        except Exception as e:
            error_msg = f"An exception occurred during analysis: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.signals.error.emit(error_msg)
        finally:
            if self._is_interrupted:
                # Ensure temporary directory is cleaned up
                try:
                    shutil.rmtree(self.temp_dir)
                    self.logger.debug(f"Temporary directory '{self.temp_dir}' has been removed due to interruption.")
                except Exception as e:
                    self.logger.warning(f"Failed to remove temporary directory '{self.temp_dir}': {e}")

    def emit_progress(self, message):
        """
        Emits a progress signal with the given message.
        """
        self.signals.progress.emit(message)
