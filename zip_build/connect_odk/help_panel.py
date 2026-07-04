from PyQt5.QtWidgets import (
    QHBoxLayout, QVBoxLayout, QPushButton, QGroupBox,
    QTextBrowser, QSplitter, QWidget,
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QGuiApplication, QDesktopServices


def plugin_dialog_parent():
    """Return the QGIS main window so dialogs stay scoped to QGIS."""
    try:
        from qgis.utils import iface
        if iface is not None:
            return iface.mainWindow()
    except Exception:
        pass
    return None


def configure_qgis_dialog(dialog, parent=None):
    """Keep plugin dialogs modal over QGIS only, not over other applications."""
    if parent is None:
        parent = plugin_dialog_parent()
    if parent is not None:
        dialog.setParent(parent)
    dialog.setWindowModality(Qt.WindowModal)
    dialog.setWindowFlags(
        Qt.Dialog | Qt.WindowTitleHint | Qt.WindowCloseButtonHint
    )
    return dialog


def resize_dialog_to_screen(dialog, min_width=480, min_height=420, max_width=860, max_height=720):
    dialog.setMinimumSize(min_width, min_height)
    screen = QGuiApplication.primaryScreen()
    if screen is None:
        dialog.resize(max(640, min_width), max(560, min_height))
        return

    available = screen.availableGeometry()
    width = max(min_width, min(max_width, int(available.width() * 0.9)))
    height = max(min_height, min(max_height, int(available.height() * 0.88)))
    dialog.resize(width, height)


class CollapsibleHelpMixin:
    def _attach_collapsible_help(self, work_panel, help_html, link_handler=None, add_toggle_row=True):
        work_layout = work_panel.layout()
        if work_layout is None:
            work_layout = QVBoxLayout(work_panel)
        work_layout.setContentsMargins(0, 0, 0, 0)
        work_layout.setSpacing(8)

        self.toggle_help_button = QPushButton("« Show Help")
        self.toggle_help_button.setToolTip("Show or hide the help panel")
        self.toggle_help_button.clicked.connect(self._toggle_help_panel)

        if add_toggle_row:
            toggle_row = QHBoxLayout()
            toggle_row.addStretch()
            toggle_row.addWidget(self.toggle_help_button)
            work_layout.insertLayout(0, toggle_row)

        self.help_box = QGroupBox()
        self.help_box.setFlat(True)
        help_layout = QVBoxLayout(self.help_box)
        help_layout.setContentsMargins(2, 2, 2, 2)
        help_layout.setSpacing(0)
        self.help_browser = QTextBrowser()
        self.help_browser.setOpenExternalLinks(False)
        self.help_browser.setHtml(help_html)
        handler = link_handler or self._default_help_link_clicked
        self.help_browser.anchorClicked.connect(handler)
        self.help_browser.setMinimumWidth(220)
        self.help_browser.setMaximumWidth(300)
        help_layout.addWidget(self.help_browser)

        self._saved_help_width = 260
        self.splitter = QSplitter(Qt.Horizontal)
        self.splitter.addWidget(work_panel)
        self.splitter.addWidget(self.help_box)
        self.splitter.setStretchFactor(0, 1)
        self.splitter.setStretchFactor(1, 0)
        self.splitter.setCollapsible(0, False)
        self.splitter.setCollapsible(1, True)
        self.splitter.setSizes([9999, 0])
        self.splitter.splitterMoved.connect(self._on_help_splitter_moved)

        outer_layout = QHBoxLayout(self)
        outer_layout.setContentsMargins(6, 6, 6, 6)
        outer_layout.addWidget(self.splitter)

    def _toggle_help_panel(self):
        sizes = self.splitter.sizes()
        if sizes[1] > 0:
            self._saved_help_width = max(sizes[1], 220)
            self.splitter.setSizes([sum(sizes), 0])
        else:
            total = sum(sizes)
            self.splitter.setSizes([max(1, total - self._saved_help_width), self._saved_help_width])
        self._update_help_toggle_label()

    def _on_help_splitter_moved(self, _pos, _index):
        self._update_help_toggle_label()

    def _update_help_toggle_label(self):
        if self.splitter.sizes()[1] > 0:
            self.toggle_help_button.setText("Hide Help »")
        else:
            self.toggle_help_button.setText("« Show Help")

    def _default_help_link_clicked(self, url):
        QDesktopServices.openUrl(url)
