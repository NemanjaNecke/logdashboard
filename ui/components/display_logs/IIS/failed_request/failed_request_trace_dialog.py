import os
from PyQt5.QtCore import Qt  # pylint: disable=no-name-in-module
from PyQt5.QtWidgets import QDockWidget, QWidget, QVBoxLayout, QTabWidget  # pylint: disable=no-name-in-module
from PyQt5.QtWebEngineWidgets import QWebEngineView  # pylint: disable=no-name-in-module
from lxml import etree
from datetime import datetime

class FrebViewerDock(QDockWidget):
    def __init__(self, xml_path, xsl_path, parent=None):
        super().__init__("FREB Trace Viewer", parent)
        self.xml_path = xml_path
        self.xsl_path = xsl_path  # (Not used here, but kept for signature compatibility.)
        self.setAllowedAreas(Qt.AllDockWidgetAreas)
        self.initUI()
        self.processXml()

    def initUI(self):
        # Create the main widget and layout.
        central = QWidget(self)
        layout = QVBoxLayout(central)
        self.mainTab = QTabWidget(central)
        layout.addWidget(self.mainTab)
        central.setLayout(layout)
        self.setWidget(central)

        # Create the three main tabs:
        self.summaryView = QWebEngineView(self)
        # Instead of a single view, create a nested QTabWidget for details:
        self.detailsTab = QTabWidget(self)
        self.compactView = QWebEngineView(self)

        self.mainTab.addTab(self.summaryView, "Request Summary")
        self.mainTab.addTab(self.detailsTab, "Request Details")
        self.mainTab.addTab(self.compactView, "Compact View")

    def processXml(self):
        # Try to parse the XML file
        try:
            tree = etree.parse(self.xml_path)
            root = tree.getroot()
        except Exception as e:
            error_html = f"<html><body><h1>Error loading XML:</h1><p>{e}</p></body></html>"
            self.summaryView.setHtml(error_html)
            self.compactView.setHtml(error_html)
            # If error occurs, also clear detailsTab (or add a placeholder)
            self.detailsTab.clear()
            placeholder = self.createWebView(error_html)
            self.detailsTab.addTab(placeholder, "Details")
            return

        # Define the namespace mapping used in the FREB XML
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}

        # Build a list of event records with some time/duration calculations
        events = root.findall("ev:Event", ns)
        prev_time = None
        event_data = []
        for event in events:
            time_elem = event.find("ev:System/ev:TimeCreated", ns)
            time_str = time_elem.get("SystemTime") if time_elem is not None else None
            current_time = datetime.fromisoformat(time_str.replace("Z", "+00:00")) if time_str else None
            duration = 0
            if prev_time and current_time:
                duration = (current_time - prev_time).total_seconds() * 1000  # in ms
            prev_time = current_time
            level = event.findtext("ev:System/ev:Level", default="N/A", namespaces=ns)
            event_data.append({
                "time": time_str,
                "duration": duration,
                "level": level,
                "event": event
            })

        # Build and set the Request Summary HTML.
        summary_html = self.build_summary(root)
        self.summaryView.setHtml(summary_html)

        # Now, rebuild the details tab with multiple sub–tabs:
        self.detailsTab.clear()
        complete_html = self.build_complete_request_trace_html(event_data)
        filter_html = self.build_filter_notifications_html(event_data)
        module_html = self.build_module_notifications_html(event_data)
        performance_html = self.build_performance_view_html(event_data)
        auth_html = self.build_authentication_authorization_html(event_data)
        aspx_html = self.build_aspx_page_traces_html(event_data)
        custom_html = self.build_custom_module_traces_html(event_data)
        fastcgi_html = self.build_fastcgi_module_html(event_data)

        self.detailsTab.addTab(self.createWebView(complete_html), "Complete Request Trace")
        self.detailsTab.addTab(self.createWebView(filter_html), "Filter Notifications")
        self.detailsTab.addTab(self.createWebView(module_html), "Module Notifications")
        self.detailsTab.addTab(self.createWebView(performance_html), "Performance View")
        self.detailsTab.addTab(self.createWebView(auth_html), "Authentication Authorization")
        self.detailsTab.addTab(self.createWebView(aspx_html), "ASP.Net Page Traces")
        self.detailsTab.addTab(self.createWebView(custom_html), "Custom Module Traces")
        self.detailsTab.addTab(self.createWebView(fastcgi_html), "FastCGI Module")

        # Build and set the Compact View HTML.
        compact_html = self.build_compact(event_data)
        self.compactView.setHtml(compact_html)

    def createWebView(self, html_content):
        view = QWebEngineView(self)
        view.setHtml(html_content)
        return view

    def build_summary(self, root):
        # Build a summary table from the attributes of <failedRequest>
        keys = [("Url", "url"), ("Site ID", "siteId"), ("App Pool", "appPoolId"),
                ("Process ID", "processId"), ("Verb", "verb"), ("Token User", "tokenUserName"),
                ("Auth Type", "authenticationType"), ("Activity ID", "activityId"),
                ("Failure Reason", "failureReason"), ("Final Status", "statusCode"),
                ("Trigger Status", "triggerStatusCode"), ("Time Taken (ms)", "timeTaken")]
        rows = ''.join([f'<tr><th>{label}</th><td>{root.get(attr, "N/A")}</td></tr>\n'
                        for label, attr in keys])
        return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #333; padding: 4px; }}
    th {{ background-color: #f0f0f0; text-align: left; }}
  </style>
</head>
<body>
  <h1>Request Summary</h1>
  <table>{rows}</table>
</body>
</html>"""

    def build_table_html(self, title, headers, rows):
        header_html = "".join([f"<th>{h}</th>" for h in headers])
        rows_html = "".join(rows)
        return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border: 1px solid #333; padding: 4px; }}
    th {{ background-color: #ddd; }}
    tr:nth-child(even) {{ background-color: #f9f9f9; }}
  </style>
  <title>{title}</title>
</head>
<body>
  <h1>{title}</h1>
  <table>
    <thead><tr>{header_html}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</body>
</html>"""
    def build_complete_request_trace_html(self, event_data):
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        html_content = """<!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <style>
        .event-container { margin-bottom: 10px; border: 1px solid #ddd; }
        .event-header { 
        padding: 8px; 
        background-color: #f5f5f5; 
        cursor: pointer; 
        display: flex; 
        justify-content: space-between;
        }
        .event-details { padding: 8px; display: none; }
        .severity { padding: 2px 5px; border-radius: 3px; }
        .severity-critical { background-color: #990000; color: white; }
        .severity-error { background-color: #ffcccc; }
        .severity-warning { background-color: #fff3cd; }
        .data-table { width: 100%; border-collapse: collapse; }
        .data-table td, .data-table th { border: 1px solid #ddd; padding: 4px; }
        .toggle { font-weight: bold; margin-right: 10px; }
    </style>
    <script>
        function toggleDetails(id) {
        const details = document.getElementById(`details_${id}`);
        const toggle = document.getElementById(`toggle_${id}`);
        if (details.style.display === 'none') {
            details.style.display = 'block';
            toggle.textContent = '-';
        } else {
            details.style.display = 'none';
            toggle.textContent = '+';
        }
        }
    </script>
    </head>
    <body>
    <h1>Complete Request Trace</h1>"""

        for idx, event in enumerate(event_data, 1):
            evt = event["event"]
            time_str = event["time"][11:23] if event["time"] else "N/A"
            rendering = evt.find("ev:RenderingInfo", namespaces=ns)
            opcode = rendering.findtext("ev:Opcode", default="N/A", namespaces=ns) if rendering is not None else "N/A"
            severity = self.get_severity(event["level"])
            
            # Extract all data elements
            data_elements = {}
            for data in evt.findall("ev:EventData/ev:Data", namespaces=ns):
                name = data.get("Name")
                value = data.text or ""
                data_elements[name] = value

            # Build data table
            data_rows = "\n".join(
                f"<tr><th>{k}</th><td>{v}</td></tr>"
                for k, v in data_elements.items()
                if k not in ["ContextId", "ConnId"]
            )

            html_content += f"""
    <div class="event-container">
        <div class="event-header" onclick="toggleDetails({idx})">
        <div>
            <span class="toggle" id="toggle_{idx}">+</span>
            <span>{idx}. {opcode}</span>
        </div>
        <div>
            <span class="severity {severity['class']}">{severity['label']}</span>
            <span>{time_str}</span>
        </div>
        </div>
        <div class="event-details" id="details_{idx}">
        <table class="data-table">
            <tbody>
            {data_rows}
            <tr><th>Duration</th><td>{event['duration']:.2f} ms</td></tr>
            </tbody>
        </table>
        </div>
    </div>"""

        html_content += "</body></html>"
        return html_content

    def build_module_notifications_html(self, event_data):
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = []
        for e in event_data:
            evt = e["event"]
            module = evt.findtext("ev:EventData/ev:Data[@Name='ModuleName']", namespaces=ns)
            notification = evt.findtext("ev:EventData/ev:Data[@Name='Notification']", namespaces=ns)
            if module and notification:
                filtered.append((e, module, notification))

        html_content = """<!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <style>
        .notification-table { width: 100%; border-collapse: collapse; }
        .notification-table th, .notification-table td { border: 1px solid #ddd; padding: 8px; }
        .details-row td { padding: 0 !important; }
        .nested-table { width: 100%; background-color: #f9f9f9; }
        .nested-table td { padding: 4px 8px; }
    </style>
    </head>
    <body>
    <h1>Module Notifications</h1>
    <table class="notification-table">
        <thead>
        <tr>
            <th>No.</th>
            <th>Module</th>
            <th>Notification</th>
            <th>Duration</th>
        </tr>
        </thead>
        <tbody>"""

        for idx, (event, module, notification) in enumerate(filtered, 1):
            evt = event["event"]
            data_elements = "\n".join(
                f"<tr><td>{data.get('Name')}</td><td>{data.text or ''}</td></tr>"
                for data in evt.findall("ev:EventData/ev:Data", namespaces=ns)
                if data.get("Name") not in ["ModuleName", "Notification"]
            )

            html_content += f"""
        <tr>
            <td>{idx}</td>
            <td>{module}</td>
            <td>{notification}</td>
            <td>{event['duration']:.2f} ms</td>
        </tr>
        <tr class="details-row">
            <td colspan="4">
            <table class="nested-table">
                <tbody>
                {data_elements}
                </tbody>
            </table>
            </td>
        </tr>"""

        html_content += "</tbody></table></body></html>"
        return html_content


    def build_performance_view_html(self, event_data):
        # For performance view, list all events with a link to view trace,
        # display the opcode and the duration (in ms, rounded to integer).
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        rows = []
        for idx, event in enumerate(event_data, 1):
            evt = event["event"]
            opcode = evt.findtext("ev:RenderingInfo/ev:Opcode", default="N/A", namespaces=ns)
            duration = event["duration"] if event.get("duration") is not None else 0
            # Create a link labeled "view trace" that (for example) jumps to an anchor with the row number.
            rows.append(f"<tr><td>{idx}. <a href='#detail_{idx}'>view trace</a></td><td>{opcode}</td><td>{duration:.0f}</td></tr>")
        headers = ["No.", "Event", "Duration (ms)"]
        return self.build_table_html("Performance View", headers, rows)

    def build_filter_notifications_html(self, event_data):
        # Filter events that have a Data element with Name 'FilterName'
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = [e for e in event_data if e["event"].find("ev:EventData/ev:Data[@Name='FilterName']", namespaces=ns) is not None]
        rows = []
        for idx, event in enumerate(filtered, 1):
            evt = event["event"]
            rendering = evt.find("ev:RenderingInfo", namespaces=ns)
            opcode = rendering.findtext("ev:Opcode", default="N/A", namespaces=ns) if rendering is not None else "N/A"
            time_str = event["time"][11:23] if event["time"] else "N/A"
            data_elements = evt.findall("ev:EventData/ev:Data", namespaces=ns)
            data_summary = ", ".join([f'{d.get("Name")}: {d.text}' for d in data_elements])
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td><td>{data_summary}</td></tr>")
        headers = ["No.", "Opcode", "Time", "Data"]
        return self.build_table_html("Filter Notifications", headers, rows)

    def build_authentication_authorization_html(self, event_data):
        # Filter events where the opcode starts with AUTH_ or SECURITY_
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = []
        for e in event_data:
            evt = e["event"]
            opcode_elem = evt.find("ev:RenderingInfo/ev:Opcode", namespaces=ns)
            opcode = opcode_elem.text if opcode_elem is not None else ""
            if opcode.startswith("AUTH_") or opcode.startswith("SECURITY_"):
                filtered.append(e)
        rows = []
        for idx, event in enumerate(filtered, 1):
            evt = event["event"]
            opcode = evt.findtext("ev:RenderingInfo/ev:Opcode", default="N/A", namespaces=ns)
            time_str = event["time"][11:23] if event["time"] else "N/A"
            data_elements = evt.findall("ev:EventData/ev:Data", namespaces=ns)
            data_summary = ", ".join([f'{d.get("Name")}: {d.text}' for d in data_elements])
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td><td>{data_summary}</td></tr>")
        headers = ["No.", "Opcode", "Time", "Data"]
        return self.build_table_html("Authentication & Authorization", headers, rows)

    def build_aspx_page_traces_html(self, event_data):
        # Filter events with opcode equal to AspNetPageTraceWarnEvent or AspNetPageTraceWriteEvent
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = [e for e in event_data if e["event"].findtext("ev:RenderingInfo/ev:Opcode", default="", namespaces=ns) in ("AspNetPageTraceWarnEvent", "AspNetPageTraceWriteEvent")]
        rows = []
        for idx, event in enumerate(filtered, 1):
            evt = event["event"]
            opcode = evt.findtext("ev:RenderingInfo/ev:Opcode", default="N/A", namespaces=ns)
            time_str = event["time"][11:23] if event["time"] else "N/A"
            data_elements = evt.findall("ev:EventData/ev:Data", namespaces=ns)
            data_summary = ", ".join([f'{d.get("Name")}: {d.text}' for d in data_elements])
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td><td>{data_summary}</td></tr>")
        headers = ["No.", "Opcode", "Time", "Data"]
        return self.build_table_html("ASP.Net Page Traces", headers, rows)

    def build_custom_module_traces_html(self, event_data):
        # For demonstration, assume custom module traces are those where the opcode contains 'ModuleDiag'
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = [e for e in event_data if "ModuleDiag" in (e["event"].findtext("ev:RenderingInfo/ev:Opcode", default="", namespaces=ns) or "")]
        rows = []
        for idx, event in enumerate(filtered, 1):
            evt = event["event"]
            opcode = evt.findtext("ev:RenderingInfo/ev:Opcode", default="N/A", namespaces=ns)
            time_str = event["time"][11:23] if event["time"] else "N/A"
            module = evt.findtext("ev:EventData/ev:Data[@Name='ModuleName']", namespaces=ns) or "N/A"
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td><td>{module}</td></tr>")
        headers = ["No.", "Opcode", "Time", "Module"]
        return self.build_table_html("Custom Module Traces", headers, rows)

    def build_fastcgi_module_html(self, event_data):
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        filtered = [e for e in event_data if e["event"].findtext("ev:RenderingInfo/ev:Opcode", default="", namespaces=ns).startswith("FASTCGI_")]
        rows = []
        for idx, event in enumerate(filtered, 1):
            evt = event["event"]
            opcode = evt.findtext("ev:RenderingInfo/ev:Opcode", default="N/A", namespaces=ns)
            time_str = event["time"][11:23] if event["time"] else "N/A"
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td></tr>")
        headers = ["No.", "Opcode", "Time"]
        return self.build_table_html("FastCGI Module", headers, rows)

    def build_compact(self, event_data):
        ns = {"ev": "http://schemas.microsoft.com/win/2004/08/events/event"}
        rows = []
        for idx, event in enumerate(event_data, 1):
            evt = event["event"]
            time_str = event["time"][11:23] if event["time"] else "N/A"
            rendering = evt.find("ev:RenderingInfo", namespaces=ns)
            opcode = rendering.findtext("ev:Opcode", default="N/A", namespaces=ns) if rendering is not None else "N/A"
            rows.append(f"<tr><td>{idx}</td><td>{opcode}</td><td>{time_str}</td></tr>")
        headers = ["No.", "Opcode", "Time"]
        return self.build_table_html("Compact View", headers, rows)

    def get_severity(self, level):
        severity_map = {
            '1': {'label': 'Critical', 'class': 'severity-critical'},
            '2': {'label': 'Error', 'class': 'severity-error'},
            '3': {'label': 'Warning', 'class': 'severity-warning'},
            '4': {'label': 'Info', 'class': ''},
            '5': {'label': 'Verbose', 'class': ''}
        }
        return severity_map.get(level, {'label': 'N/A', 'class': ''})
