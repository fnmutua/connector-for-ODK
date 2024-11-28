from PyQt5.QtWidgets import QDialog, QVBoxLayout, QPushButton, QLabel, QComboBox, QMessageBox
from qgis.core import (
    QgsProject,
    QgsVectorLayer,
    QgsFeatureRequest,
    QgsFields,
    QgsFeature,
    QgsWkbTypes,
)

class SplitLayerDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Split Layer")
        self.setFixedSize(300, 150)

        # Layout and widgets for the dialog
        layout = QVBoxLayout()

        # Label
        self.label = QLabel("Choose a layer to split and set criteria")
        
        # ComboBox to select a layer
        self.layer_combobox = QComboBox()
        self.populate_layers()

        # ComboBox to select an attribute
        self.attribute_combobox = QComboBox()
        self.attribute_combobox.setEnabled(False)

        # Split Button
        self.split_button = QPushButton("Split Layer")
        self.split_button.setEnabled(False)

        # Add widgets to layout
        layout.addWidget(self.label)
        layout.addWidget(self.layer_combobox)
        layout.addWidget(self.attribute_combobox)
        layout.addWidget(self.split_button)
        
        self.setLayout(layout)

        # Connect the ComboBox signals to methods
        self.layer_combobox.currentIndexChanged.connect(self.on_layer_selected)
        self.split_button.clicked.connect(self.split_layer)

        # Automatically select the first project (index 0) and trigger the on_layer_selected method manually
        if self.layer_combobox.count() > 0:
            self.layer_combobox.setCurrentIndex(0)
            self.on_layer_selected()  # Now safe to call

    def populate_layers(self):
        """Populate the ComboBox with the available layers in the QGIS project."""
        layers = QgsProject.instance().mapLayers().values()
        for layer in layers:
            if isinstance(layer, QgsVectorLayer):  # Only vector layers
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

        # Get unique values in the selected attribute, excluding NULL
        unique_values = set()
        for feature in layer.getFeatures():
            value = feature[selected_attribute]
            if value is not None:  # Exclude NULL values
                unique_values.add(value)

        if not unique_values:
            QMessageBox.warning(self, "Error", "No valid unique values found in the selected field.")
            return

        # Create a new layer for each unique value
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
        # Filter features that match the attribute value
        request = QgsFeatureRequest().setFilterExpression(f'"{attribute}" = \'{value}\'')
        features = list(layer.getFeatures(request))  # Collect features into a list

        if not features:
            return None  # Return None if no features match the criteria

        # Define the CRS and fields for the new layer
        crs = layer.crs().toWkt()
        original_fields = layer.fields()
        geometry_type = layer.geometryType()
        wkb_type = layer.wkbType()

        # Identify fields that are not completely empty
        def is_field_non_empty(field_name):
            for feature in features:
                value = feature[field_name]
                if value not in [None, '', [], {}, False]:  # Consider these as empty
                    return True
            return False

        non_null_fields = QgsFields()
        for field in original_fields:
            if is_field_non_empty(field.name()):
                non_null_fields.append(field)

        if not non_null_fields:
            return None  # Return None if all fields are completely empty

        # Create a memory layer
        new_layer = QgsVectorLayer(f"{QgsWkbTypes.displayString(wkb_type)}?crs={crs}", f"{layer.name()}_{value}", "memory")
        new_layer_data_provider = new_layer.dataProvider()

        # Add only the non-empty fields to the new layer
        new_layer_data_provider.addAttributes(non_null_fields)
        new_layer.updateFields()

        # Add filtered features to the new layer
        for feature in features:
            new_feature = QgsFeature()
            new_feature.setGeometry(feature.geometry())  # Copy geometry
            new_feature.setFields(non_null_fields)  # Set the fields to the new feature

            # Set attributes only for non-null fields
            attributes = [feature[field.name()] for field in non_null_fields]
            new_feature.setAttributes(attributes)
            new_layer_data_provider.addFeature(new_feature)

        new_layer.updateExtents()
        return new_layer
