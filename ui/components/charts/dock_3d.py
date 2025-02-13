# ui/dock_3d.py
from PyQt5.QtWidgets import QDockWidget # pylint: disable=no-name-in-module
from PyQt5.QtWebEngineWidgets import QWebEngineView # pylint: disable=no-name-in-module
from PyQt5.QtCore import Qt # pylint: disable=no-name-in-module
import plotly.graph_objects as go
import plotly.io as pio
import logging

class ThreeDDock(QDockWidget):
    def __init__(self, parent=None):
        super().__init__("3D Visualization", parent)
        self.setAllowedAreas(Qt.AllDockWidgetAreas)

        self.web_view = QWebEngineView()
        self.setWidget(self.web_view)

        # Example: create a figure
        fig = go.Figure(data=[go.Scatter3d(
            x=[1,2,3], 
            y=[2,5,1], 
            z=[10,3,8],
            mode='markers',
            marker=dict(size=5, color=[10,3,8], colorscale='Bluered')
        )])
        fig.update_layout(title="3D Example in Dock")

        # Convert to HTML
        html = pio.to_html(fig, full_html=False)
        self.web_view.setHtml(html) 