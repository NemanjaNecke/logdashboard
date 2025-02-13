# services/converters/delegate_status.py

from PyQt5.QtWidgets import QStyledItemDelegate # pylint: disable=no-name-in-module
from PyQt5.QtGui import QColor, QIcon, QPainter # pylint: disable=no-name-in-module
from PyQt5.QtCore import Qt, QRect # pylint: disable=no-name-in-module
import os
import logging


class StatusDelegate(QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent)
        # Use the main application logger
        self.logger = logging.getLogger('DelegateStatus') # pylint: disable=no-member  # Ensure this matches the main logger name
        self.logger.setLevel(logging.DEBUG) # pylint: disable=no-member
        
        # Load icons relative to the project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        self.icons = {}
        icon_files = {
            "success": "success.png",
            "redirect": "redirect.png",
            "fail": "fail.png",
            "information": "information.png"
        }
        for key, filename in icon_files.items():
            icon_path = os.path.join(project_root, "resources", "icons", filename)
            if os.path.exists(icon_path):
                self.icons[key] = QIcon(icon_path)
                self.logger.debug(f"Loaded icon for '{key}' from '{icon_path}'.")
            else:
                self.icons[key] = QIcon()  # Empty icon to avoid crashes
                self.logger.error(f"Icon file not found for '{key}' at '{icon_path}'.")

    def paint(self, painter, option, index):
        # Get the status code
        status_code = index.data(Qt.DisplayRole)
        if status_code is None:
            status_code = ""

        # Determine the category
        try:
            code_int = int(status_code)
        except ValueError:
            code_int = 0

        if 200 <= code_int < 300:
            category = "success"
            bg_color = QColor("#70f944")  # Light green
        elif 300 <= code_int < 400:
            category = "redirect"
            bg_color = QColor("#FFECB3")  # Light orange
        elif 400 <= code_int < 600:
            category = "fail"
            bg_color = QColor("#FFCDD2")  # Light red
        else:
            category = "information"
            bg_color = QColor("#E1BEE7")  # Light purple

        # Fill the background
        painter.save()
        painter.fillRect(option.rect, bg_color)
        painter.restore()

        # Draw the icon
        icon = self.icons.get(category)
        if icon and not icon.isNull():
            icon_size = 16  # Define a standard icon size
            # Define padding
            padding_left = 5
            padding_right = 5
            # Calculate vertical position to center the icon
            y_pos = option.rect.top() + (option.rect.height() - icon_size) // 2
            # Define the icon's rectangle
            icon_rect = QRect(option.rect.left() + padding_left, y_pos, icon_size, icon_size)
            # Ensure the icon does not exceed the cell's right boundary
            if icon_rect.right() + padding_right > option.rect.right():
                icon_rect.setRight(option.rect.right() - padding_right)
            # Paint the icon
            icon.paint(painter, icon_rect, Qt.AlignVCenter | Qt.AlignLeft)
            self.logger.debug(f"Drew '{category}' icon at '{icon_rect}'.")
        else:
            self.logger.warning(f"No valid icon found for category '{category}'.")

        # Draw the text, shifted to the right of the icon
        painter.save()
        # Define spacing between icon and text
        spacing = 5
        # Calculate the starting x position for the text
        text_x = option.rect.left() + padding_left + icon_size + spacing
        text_rect = QRect(text_x, option.rect.top(),
                          option.rect.width() - (text_x - option.rect.left()) - padding_right,
                          option.rect.height())
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, status_code)
        painter.restore()
