from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QComboBox,
    QMessageBox, QWidget, QFormLayout,
)
from PyQt5.QtCore import Qt
from qgis.core import (
    QgsProject,
    QgsVectorLayer,
    QgsFeatureRequest,
    QgsFields,
    QgsFeature,
    QgsWkbTypes,
)

from .help_panel import CollapsibleHelpMixin, resize_dialog_to_screen, configure_qgis_dialog


class SplitLayerDialog(QDialog, CollapsibleHelpMixin):
    def __init__(self, parent=None):
        super().__init__(parent)
        configure_qgis_dialog(self, parent)
        self.setWindowTitle("Split Layer")
        resize_dialog_to_screen(self, min_width=320, min_height=140, max_width=560, max_height=260)

        work_panel = QWidget()
        layout = QVBoxLayout(work_panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(6)
        form.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        self.layer_combobox = QComboBox()
        self.populate_layers()
        form.addRow("Layer:", self.layer_combobox)

        self.attribute_combobox = QComboBox()
        self.attribute_combobox.setEnabled(False)
        form.addRow("Field:", self.attribute_combobox)
        layout.addLayout(form)

        button_row = QHBoxLayout()
        button_row.setSpacing(8)
        self.split_button = QPushButton("Split Layer")
        self.split_button.setEnabled(False)
        button_row.addWidget(self.split_button)
        button_row.addStretch()
        layout.addLayout(button_row)

        self._attach_collapsible_help(work_panel, self._help_html(), add_toggle_row=False)
        button_row.addWidget(self.toggle_help_button)

        self.layer_combobox.currentIndexChanged.connect(self.on_layer_selected)
        self.split_button.clicked.connect(self.split_layer)

        if self.layer_combobox.count() > 0:
            self.layer_combobox.setCurrentIndex(0)
            self.on_layer_selected()

    @staticmethod
    def _help_html():
        return """
        <h3>Split Layer</h3>
        <p>Create separate layers for each unique value in a chosen attribute field.</p>

        <h4>Quick start</h4>
        <ol>
            <li>Select a <b>vector layer</b> from the project.</li>
            <li>Select the <b>attribute field</b> to split on.</li>
            <li>Click <b>Split Layer</b>.</li>
        </ol>

        <h4>What happens</h4>
        <ul>
            <li>One new in-memory layer is created for each unique non-null value.</li>
            <li>New layers are named <code>{layer}_{value}</code>.</li>
            <li>Fields that are completely empty in a split are dropped.</li>
            <li>Geometry and CRS are copied from the source layer.</li>
        </ul>

        <h4>Notes</h4>
        <p>NULL values are ignored. Only vector layers in the current QGIS project are listed.</p>
        """

    def populate_layers(self):
        """Populate the ComboBox with the available layers in the QGIS project."""
        layers = QgsProject.instance().mapLayers().values()
        for layer in layers:
            if isinstance(layer, QgsVectorLayer):
                self.layer_combobox.addItem(layer.name(), layer.id())

    def on_layer_selected(self):
        """Populate the attribute combo box with the selected layer's attributes."""
        self.attribute_combobox.clear()
        self.attribute_combobox.setEnabled(False)
        self.split_button.setEnabled(False)

        layer_id = self.layer_combobox.currentData()
        layer = QgsProject.instance().mapLayer(layer_id)

        if layer and isinstance(layer, QgsVectorLayer):
            fields = layer.fields()
            for field in fields:
                self.attribute_combobox.addItem(field.name())
            self.attribute_combobox.setEnabled(True)
            self.split_button.setEnabled(True)

    def split_layer(self):
        """Split the selected layer based on the unique values in the selected attribute."""
        layer_id = self.layer_combobox.currentData()
        selected_attribute = self.attribute_combobox.currentText()
        layer = QgsProject.instance().mapLayer(layer_id)

        if not layer or not selected_attribute:
            QMessageBox.warning(self, "Error", "Invalid layer or attribute selection.")
            return

        unique_values = set()
        for feature in layer.getFeatures():
            value = feature[selected_attribute]
            if value is not None:
                unique_values.add(value)

        if not unique_values:
            QMessageBox.warning(self, "Error", "No valid unique values found in the selected field.")
            return

        for value in unique_values:
            new_layer = self.create_layer_for_value(layer, selected_attribute, value)
            if new_layer:
                QgsProject.instance().addMapLayer(new_layer)

        QMessageBox.information(self, "Success", f"Layer split into {len(unique_values)} layers.")

    def create_layer_for_value(self, layer, attribute, value):
        """
        Create a new layer containing features that match the specified attribute value,
        removing fields that are completely empty.
        """
        request = QgsFeatureRequest().setFilterExpression(f'"{attribute}" = \'{value}\'')
        features = list(layer.getFeatures(request))

        if not features:
            return None

        crs = layer.crs().toWkt()
        original_fields = layer.fields()
        wkb_type = layer.wkbType()

        def is_field_non_empty(field_name):
            for feature in features:
                value = feature[field_name]
                if value not in [None, '', [], {}, False]:
                    return True
            return False

        non_null_fields = QgsFields()
        for field in original_fields:
            if is_field_non_empty(field.name()):
                non_null_fields.append(field)

        if not non_null_fields:
            return None

        new_layer = QgsVectorLayer(
            f"{QgsWkbTypes.displayString(wkb_type)}?crs={crs}",
            f"{layer.name()}_{value}",
            "memory",
        )
        new_layer_data_provider = new_layer.dataProvider()

        new_layer_data_provider.addAttributes(non_null_fields)
        new_layer.updateFields()

        for feature in features:
            new_feature = QgsFeature()
            new_feature.setGeometry(feature.geometry())
            new_feature.setFields(non_null_fields)
            attributes = [feature[field.name()] for field in non_null_fields]
            new_feature.setAttributes(attributes)
            new_layer_data_provider.addFeature(new_feature)

        new_layer.updateExtents()
        return new_layer
