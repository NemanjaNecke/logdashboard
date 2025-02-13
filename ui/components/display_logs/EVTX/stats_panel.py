# ui/components/display_logs/EVTX/stats_panel.py
from PyQt5.QtWidgets import QTreeWidget, QTreeWidgetItem
from PyQt5.QtCore import Qt

class StatsPanel:
    def __init__(self, tree_widget: QTreeWidget, chunk_mode: bool = False, chunk_size: int = 50):
        """
        Initialize the stats panel helper.
        
        :param tree_widget: The QTreeWidget where the stats will be displayed.
        :param chunk_mode: If True, insert children in chunks (to avoid UI freezes on huge datasets).
                           If False, insert immediately.
        :param chunk_size: Number of child items to insert per event-loop iteration (used in chunk_mode).
        """
        self.tree_widget = tree_widget
        self.chunk_mode = chunk_mode
        self.chunk_size = chunk_size
        self._insertion_queue = []
        self.stats_data = {}  # Holds the raw stats data
        
        # For lazy loading (optional), you can connect the itemExpanded signal:
        self.tree_widget.itemExpanded.connect(self.populateItemChildren)

    def clear(self):
        """Clear the tree widget and internal queue."""
        self.tree_widget.clear()
        self._insertion_queue = []
        self.stats_data = {}

    def setStats(self, stats: dict):
        """
        Populate the stats panel with the given statistics dictionary.
        The stats dict should be in the form:
           { field_name: { value: count, ... }, ... }
        """
        self.clear()
        self.stats_data = stats  # Save the raw stats for lazy loading

        # Create a top-level item for each field and add a dummy child.
        for field, value_counts in stats.items():
            parent_item = QTreeWidgetItem([field, ""])
            # Save the field in the UserRole data for later use
            parent_item.setData(0, Qt.UserRole, field)
            # Add a dummy child so the expand arrow appears.
            dummy = QTreeWidgetItem(["Loading...", ""])
            parent_item.addChild(dummy)
            self.tree_widget.addTopLevelItem(parent_item)
        # Do not automatically expand

    def populateItemChildren(self, item: QTreeWidgetItem):
        """
        When the user expands a top-level item, remove the dummy and insert the actual children.
        """
        field = item.data(0, Qt.UserRole)
        # If the children are already populated (i.e. not just the dummy), do nothing.
        if item.childCount() > 0 and item.child(0).text(0) != "Loading...":
            return

        # Remove the dummy child.
        item.takeChildren()

        # Get the stats for this field.
        value_counts = self.stats_data.get(field, {})
        # Sort values descending by count.
        sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
        for val, cnt in sorted_values:
            child_item = QTreeWidgetItem([str(val), str(cnt)])
            # Store the (field, value) pair for later filtering.
            child_item.setData(0, Qt.UserRole, (field, str(val)))
            item.addChild(child_item)
