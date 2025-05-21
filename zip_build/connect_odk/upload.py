import subprocess
import sys
import requests
from PyQt5.QtWidgets import (QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, 
                             QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, 
                             QGroupBox, QTextEdit, QScrollArea, QGridLayout, QWidget, QTableWidget, 
                             QTableWidgetItem, QSizePolicy)
from PyQt5.QtCore import QVariant, QSettings, Qt, QThread, pyqtSignal, QObject
from fuzzywuzzy import fuzz
import json
import geopandas as gpd
import pandas as pd
from qgis.core import QgsVectorLayer, QgsProject
import shortuuid

class Worker(QObject):
    """Worker object to run fetch_pcode_data in a background thread."""
    progress = pyqtSignal(int)  # Emit progress percentage (for pcode lookup)
    log = pyqtSignal(str)  # Emit log messages
    finished = pyqtSignal()  # Signal when done
    result = pyqtSignal(dict, list)  # Emit pcode_entity_data and valid_feature_indices

    def __init__(self, layer, parent_entity_name, url, token):
        super().__init__()
        self.layer = layer
        self.parent_entity_name = parent_entity_name
        self.url = url
        self.token = token
        self.gdf = None

    def run(self):
        """Fetch pcode-based entity data in the background."""
        try:
            layer_fields = [f.name() for f in self.layer.fields()]
            use_geometry_lookup = 'pcode' not in layer_fields
            if not use_geometry_lookup:
                self.log.emit("'code' column found in layer." if "code" in layer_fields else "No 'code' column found. Generating unique codes using shortid.")
            
            srid = self.layer.crs().postgisSrid()
            self.log.emit(f"Layer CRS SRID detected: {srid}")

            pcode_entity_data = {}
            valid_feature_indices = []
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }

            if use_geometry_lookup:
                # Batch geometry-based lookup
                geometries = [row["geojson"] for _, row in self.gdf.iterrows() if row["geojson"] is not None]
                if not geometries:
                    self.log.emit("No valid geometries found for intersection.")
                    self.result.emit(pcode_entity_data, valid_feature_indices)
                    self.finished.emit()
                    return

                # No progress signals for geometry lookup (handled by indeterminate progress bar)
                try:
                    response = requests.post(
                        f"{self.url}/api/v1/data/intersect",
                        headers=headers,
                        json={
                            "model": self.parent_entity_name,
                            "geometry": geometries,
                            "srid": srid
                        }
                    )
                    if response.status_code == 200:
                        entity_data = response.json()
                        results = entity_data.get("data", [])
                        self.log.emit(f"Received {entity_data.get('count', 0)} intersecting records from batch query.")

                        # Map results to features using geometry_index
                        index_to_row = {i: row_idx for i, (row_idx, _) in enumerate(self.gdf.iterrows()) if self.gdf.loc[row_idx, "geojson"] is not None}
                        for result in results:
                            geometry_index = result.get("geometry_index")
                            records = result.get("records", [])
                            if geometry_index in index_to_row:
                                row_idx = index_to_row[geometry_index]
                                if records:
                                    record = records[0]  # Take first record if multiple
                                    if record.get("id"):
                                        id_key = None
                                        parent_entity_lower = self.parent_entity_name.lower()
                                        if parent_entity_lower == "settlement":
                                            id_key = "settlement_id"
                                        elif parent_entity_lower == "ward":
                                            id_key = "ward_id"
                                        elif parent_entity_lower == "subcounty":
                                            id_key = "subcounty_id"
                                        elif parent_entity_lower == "county":
                                            id_key = "county_id"

                                        if id_key:
                                            pcode_entity_data[row_idx] = {
                                                id_key: record.get("id"),
                                                **({"settlement_id": record.get("settlement_id")} if id_key != "settlement_id" else {}),
                                                **({"ward_id": record.get("ward_id")} if id_key != "ward_id" else {}),
                                                **({"subcounty_id": record.get("subcounty_id")} if id_key != "subcounty_id" else {}),
                                                **({"county_id": record.get("county_id")} if id_key != "county_id" else {})
                                            }
                                            valid_feature_indices.append(row_idx)
                                            self.log.emit(f"Assigned {parent_entity_lower}-based data for index {row_idx}: {pcode_entity_data[row_idx]}")
                                        else:
                                            self.log.emit(f"Error: Invalid parent entity '{self.parent_entity_name}' for index {row_idx}")
                                else:
                                    self.log.emit(f"No intersect result for geometry at index {row_idx}")
                            else:
                                self.log.emit(f"Invalid geometry_index {geometry_index} in response")
                    else:
                        self.log.emit(f"Failed to fetch batch geometry data: {response.text}")
                except Exception as e:
                    self.log.emit(f"Error fetching batch geometry data: {str(e)}")
            else:
                # Pcode-based lookup (individual requests)
                total_features = sum(1 for _ in self.gdf.iterrows())
                for idx, (row_idx, row) in enumerate(self.gdf.iterrows()):
                    pcode = row["pcode"] if "pcode" in row else None
                    if not pcode:
                        self.log.emit(f"No pcode for feature index {row_idx}, skipped.")
                        continue

                    try:
                        response = requests.post(
                            f"{self.url}/api/v1/data/one/code",
                            headers=headers,
                            json={"model": self.parent_entity_name, "code": pcode}
                        )
                        if response.status_code == 200:
                            entity_data = response.json()
                            record = entity_data.get("data", {})
                            if record and record.get("id"):
                                id_key = None
                                parent_entity_lower = self.parent_entity_name.lower()
                                if parent_entity_lower == "settlement":
                                    id_key = "settlement_id"
                                elif parent_entity_lower == "ward":
                                    id_key = "ward_id"
                                elif parent_entity_lower == "subcounty":
                                    id_key = "subcounty_id"
                                elif parent_entity_lower == "county":
                                    id_key = "county_id"

                                if id_key:
                                    pcode_entity_data[row_idx] = {
                                        id_key: record.get("id"),
                                        "settlement_id": record.get("settlement_id"),
                                        "ward_id": record.get("ward_id"),
                                        "subcounty_id": record.get("subcounty_id"),
                                        "county_id": record.get("county_id")
                                    }
                                    valid_feature_indices.append(row_idx)
                                    self.log.emit(f"Fetched pcode-based data for index {row_idx}: {pcode_entity_data[row_idx]}")
                            else:
                                self.log.emit(f"No matching data found for pcode '{pcode}' at index {row_idx}")
                        else:
                            self.log.emit(f"Failed to fetch entity data for pcode {pcode}: {response.text}")
                    except Exception as e:
                        self.log.emit(f"Error fetching entity data for pcode {pcode}: {str(e)}")

                    # Emit progress for pcode lookup
                    progress = int((idx + 1) / total_features * 100)
                    self.progress.emit(progress)

            if pcode_entity_data:
                self.log.emit(f"Data fetched successfully for {len(valid_feature_indices)} features with parent entity '{self.parent_entity_name}'.")
            else:
                self.log.emit(f"No data fetched for parent entity '{self.parent_entity_name}'.")

            self.result.emit(pcode_entity_data, valid_feature_indices)
            self.finished.emit()

        except Exception as e:
            self.log.emit(f"Error fetching pcode data: {str(e)}")
            self.finished.emit()

class KesMISDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Export data to KeSMIS")
        self.setMinimumSize(800, 700)

        # Initialize variables
        self.token = None
        self.api_entities = []
        self.field_mapping = {}
        self.pcode_entity_data = {}
        self.pcode_fields = ["settlement_id", "ward_id", "subcounty_id", "county_id"]
        self.is_logged_in = False
        self.settings = QSettings("YourOrganization", "KesMIS")
        self.valid_feature_indices = []
        self.gdf = None
        
        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # Horizontal layout for Server Login and Layer/Parent Selection
        top_layout = QHBoxLayout()

        # Server Login Section (50% width)
        login_box = QGroupBox("Server Login")
        login_layout = QGridLayout()
        
        self.url_input = QLineEdit()
        self.url_input.setText("http://localhost")
        self.url_input.setPlaceholderText("Enter server URL")
        self.username_input = QLineEdit()
        self.username_input.setText(self.settings.value("username", ""))
        self.username_input.setPlaceholderText("Username")
        self.password_input = QLineEdit()
        self.password_input.setText(self.settings.value("password", ""))
        self.password_input.setPlaceholderText("Password")
        self.password_input.setEchoMode(QLineEdit.Password)
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.login_to_server)
        self.save_credentials = QCheckBox("Save Credentials")
        self.save_credentials.stateChanged.connect(self.on_save_credentials_changed)

        login_layout.addWidget(QLabel("Server URL:"), 0, 0)
        login_layout.addWidget(self.url_input, 0, 1)
        login_layout.addWidget(QLabel("Username:"), 1, 0)
        login_layout.addWidget(self.username_input, 1, 1)
        login_layout.addWidget(QLabel("Password:"), 2, 0)
        login_layout.addWidget(self.password_input, 2, 1)
        login_layout.addWidget(self.login_button, 3, 0)
        login_layout.addWidget(self.save_credentials, 3, 1)
        login_box.setLayout(login_layout)
        login_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        top_layout.addWidget(login_box, 1)

        # Layer and Parent Selection (50% width)
        layer_box = QGroupBox("Layer and Parent Selection")
        layer_layout = QVBoxLayout()
        
        layer_selection_layout = QHBoxLayout()
        self.layer_combo = QComboBox()
        self.layer_combo.setEnabled(False)
        self.layer_combo.currentTextChanged.connect(self.reset_data)
        layer_selection_layout.addWidget(QLabel("Select Layer:"))
        layer_selection_layout.addWidget(self.layer_combo)
        
        parent_selection_layout = QHBoxLayout()
        self.parent_combo = QComboBox()
        self.parent_combo.addItems(["", "settlement", "ward"])
        self.parent_combo.setCurrentIndex(0)
        self.parent_combo.setEnabled(False)
        self.parent_combo.currentTextChanged.connect(self.start_fetch_pcode_data)
        parent_selection_layout.addWidget(QLabel("Select Parent Entity:"))
        parent_selection_layout.addWidget(self.parent_combo)
        
        entity_selection_layout = QHBoxLayout()
        self.entity_combo = QComboBox()
        self.entity_combo.setEnabled(False)
        self.entity_combo.currentTextChanged.connect(self.match_fields)
        entity_selection_layout.addWidget(QLabel("Select Entity:"))
        entity_selection_layout.addWidget(self.entity_combo)
        
        layer_layout.addLayout(layer_selection_layout)
        layer_layout.addLayout(parent_selection_layout)
        layer_layout.addLayout(entity_selection_layout)
        layer_box.setLayout(layer_layout)
        layer_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        top_layout.addWidget(layer_box, 1)

        # Field Mapping Table (100% width)
        mapping_box = QGroupBox("Field Mapping")
        mapping_layout = QVBoxLayout()
        
        self.mapping_table = QTableWidget()
        self.mapping_table.setColumnCount(3)
        self.mapping_table.setHorizontalHeaderLabels(["Layer Field", "API Field", "Match Score"])
        self.mapping_table.setFixedHeight(250)
        self.mapping_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        mapping_layout.addWidget(self.mapping_table)
        
        self.submit_button = QPushButton("Submit Data to KeSMIS")
        self.submit_button.setEnabled(False)
        self.submit_button.clicked.connect(self.submit_features)
        mapping_layout.addWidget(self.submit_button)
        
        mapping_box.setLayout(mapping_layout)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setVisible(False)

        # Log Display (100% width at bottom)
        log_box = QGroupBox("Log")
        log_layout = QVBoxLayout()
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        self.log_textedit.setFixedHeight(100)
        self.log_textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.log_textedit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        log_layout.addWidget(self.log_textedit)
        
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)
        log_layout.addWidget(self.clear_log_button)
        log_box.setLayout(log_layout)

        # Add to main layout
        main_layout.addLayout(top_layout)
        main_layout.addWidget(mapping_box)
        main_layout.addWidget(self.progress_bar)
        main_layout.addWidget(log_box)
        
        # Create scroll area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(main_widget)
        
        # Set main dialog layout
        dialog_layout = QVBoxLayout()
        dialog_layout.addWidget(scroll_area)
        self.setLayout(dialog_layout)

        # Thread for background processing
        self.thread = QThread()
        self.worker = None

    def clear_log(self):
        """Clear all messages in the log window."""
        self.log_textedit.clear()
        
    def _convert_to_serializable(self, value):
        """Convert QVariant and other non-serializable types to JSON-serializable types."""
        if isinstance(value, QVariant):
            if value.isNull():
                return None
            return value.toPyObject()
        elif isinstance(value, (list, tuple)):
            return [self._convert_to_serializable(item) for item in value]
        elif isinstance(value, dict):
            return {k: self._convert_to_serializable(v) for k, v in value.items()}
        elif isinstance(value, (int, float, str, bool)) or value is None:
            return value
        else:
            return str(value)

    def reset_data(self):
        """Clear data when the layer changes and re-fetch parent IDs if a parent is selected."""
        self.pcode_entity_data = {}
        self.valid_feature_indices = []
        self.field_mapping = {}
        self.mapping_table.setRowCount(0)
        self.submit_button.setEnabled(False)
        
        # Create and cache GeoDataFrame
        layer = self.layer_combo.currentData()
        if layer:
            try:
                self.gdf = gpd.GeoDataFrame.from_features([
                    {
                        "type": "Feature",
                        "geometry": json.loads(f.geometry().asJson()) if f.geometry() else None,
                        "properties": {field: self._convert_to_serializable(f[field]) for field in [f.name() for f in layer.fields()]}
                    } for f in layer.getFeatures()
                ])
                if self.gdf.empty:
                    self.gdf = None
                    self.log_message("Selected layer contains no features.")
                    QMessageBox.warning(self, "No Features", "The selected layer contains no features.")
                    return
                if "code" not in self.gdf.columns:
                    self.gdf["code"] = [shortuuid.ShortUUID().random(length=6) for _ in range(len(self.gdf))]
                    self.log_message("Generated unique codes for all features.")
                # Pre-serialize geometries to GeoJSON
                self.gdf["geojson"] = self.gdf.geometry.apply(lambda geom: self._convert_to_serializable(geom.__geo_interface__) if geom else None)
                self.log_message("Cached GeoDataFrame for new layer.")
            except Exception as e:
                self.gdf = None
                self.log_message(f"Error creating GeoDataFrame: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to create GeoDataFrame: {str(e)}")
                return
        else:
            self.gdf = None
            self.log_message("No valid layer selected.")
        
        self.log_message("Cleared pcode data and field mappings due to layer change.")
        
        # Re-fetch parent IDs if a parent entity is selected
        if self.parent_combo.currentText():
            self.start_fetch_pcode_data()

    def start_fetch_pcode_data(self):
        """Start fetching pcode data in a background thread."""
        if not self.layer_combo.currentData() or not self.parent_combo.currentText():
            return

        # Clear previous data to prevent stale references
        self.pcode_entity_data = {}
        self.valid_feature_indices = []
        self.log_message(f"Starting pcode data fetch for parent entity '{self.parent_combo.currentText()}'")

        # Disable UI elements during processing
        self.layer_combo.setEnabled(False)
        self.parent_combo.setEnabled(False)
        self.entity_combo.setEnabled(False)
        self.submit_button.setEnabled(False)
        self.progress_bar.setVisible(True)

        # Set indeterminate mode for geometry lookup, percentage for pcode lookup
        layer_fields = [f.name() for f in self.layer_combo.currentData().fields()]
        use_geometry_lookup = 'pcode' not in layer_fields
        if use_geometry_lookup:
            self.progress_bar.setRange(0, 0)  # Indeterminate mode (animated)
        else:
            self.progress_bar.setRange(0, 100)  # Percentage mode for pcode lookup
            self.progress_bar.setValue(0)

        # Create worker and move to thread
        self.worker = Worker(
            self.layer_combo.currentData(),
            self.parent_combo.currentText(),
            self.url_input.text(),
            self.token
        )
        self.worker.gdf = self.gdf
        self.worker.moveToThread(self.thread)

        # Connect signals
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.log.connect(self.log_message)
        self.worker.result.connect(self.on_fetch_pcode_data_finished)
        self.worker.finished.connect(self.on_worker_finished)

        # Start thread
        self.thread.started.connect(self.worker.run)
        self.thread.start()

    def on_fetch_pcode_data_finished(self, pcode_entity_data, valid_feature_indices):
        """Handle results from background worker."""
        self.pcode_entity_data = pcode_entity_data
        self.valid_feature_indices = valid_feature_indices
        if not pcode_entity_data:
            self.submit_button.setEnabled(False)
        else:
            self.submit_button.setEnabled(True)

    def on_worker_finished(self):
        """Clean up after worker finishes."""
        self.layer_combo.setEnabled(True)
        self.parent_combo.setEnabled(True)
        self.entity_combo.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)  # Reset to percentage mode
        self.thread.quit()
        self.thread.wait()
        self.worker = None

    def login_to_server(self):
        """Login to the server and get token."""
        try:
            url = self.url_input.text()
            username = self.username_input.text()
            password = self.password_input.text()

            login_url = f"{url}/api/auth/signin"
            response = requests.post(login_url, json={
                "username": username,
                "password": password
            })

            if response.status_code == 200:
                self.token = response.json().get("data")
                self.is_logged_in = True
                self.log_message("Login successful!")
                
                if self.save_credentials.isChecked():
                    self.save_credentials_to_settings()
                
                self.layer_combo.setEnabled(True)
                self.parent_combo.setEnabled(True)
                self.populate_layers()
                self.fetch_entities(url)
            else:
                self.is_logged_in = False
                self.log_message(f"Login failed: {response.text}")
                QMessageBox.critical(self, "Login Error", "Failed to login. Please check credentials.")
        except Exception as e:
            self.is_logged_in = False
            self.log_message(f"Login error: {str(e)}")
            QMessageBox.critical(self, "Error", str(e))

    def on_save_credentials_changed(self, state):
        """Handle checkbox state change for saving credentials."""
        if state == 2 and self.is_logged_in:
            self.save_credentials_to_settings()

    def save_credentials_to_settings(self):
        """Save the entered credentials to QSettings."""
        url = self.url_input.text()
        username = self.username_input.text()
        password = self.password_input.text()

        self.settings.setValue("url", url)
        self.settings.setValue("username", username)
        self.settings.setValue("password", password)

        QMessageBox.information(self, "Success", "Credentials saved successfully!")
        self.log_message(f"Saved credentials: URL={url}, Username={username}")

    def populate_layers(self):
        """Populate available layers from QGIS canvas and check for 'code' column."""
        self.layer_combo.clear()
        layers = QgsProject.instance().mapLayers().values()
        for layer in layers:
            if isinstance(layer, QgsVectorLayer):
                self.layer_combo.addItem(layer.name(), layer)
                layer_fields = [f.name() for f in layer.fields()]
                if "code" not in layer_fields:
                    self.log_message(f"Layer '{layer.name()}' does not have a 'code' column. Unique codes will be generated using shortid.")
        self.layer_combo.setEnabled(True)

    def fetch_entities(self, base_url):
        """Fetch entities from API and populate only the entity combo box."""
        try:
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }
            response = requests.get(f"{base_url}/api/v1/models/list", headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                self.api_entities = data.get("models", [])
                self.entity_combo.clear()
                self.entity_combo.addItem("")
                for entity in self.api_entities:
                    self.entity_combo.addItem(entity["model"], entity)
                self.entity_combo.setCurrentIndex(0)
                self.entity_combo.setEnabled(True)
                self.log_message("Entities fetched successfully")
            else:
                self.log_message(f"Failed to fetch entities: {response.text}")
        except Exception as e:
            self.log_message(f"Error fetching entities: {str(e)}")

    def match_fields(self):
        """Perform one-to-one fuzzy matching with minimum 70% score, allowing unmatched fields."""
        try:
            if not self.layer_combo.currentData() or not self.entity_combo.currentData():
                return

            layer = self.layer_combo.currentData()
            entity = self.entity_combo.currentData()
            
            layer_fields = [f.name() for f in layer.fields()]
            fields_to_match = layer_fields + self.pcode_fields
            api_fields = [attr["name"] for attr in entity.get("attributes", [])]
            
            self.field_mapping = {}
            self.mapping_table.setRowCount(0)
            
            match_scores = []
            for field in fields_to_match:
                for api_field in api_fields:
                    score = fuzz.ratio(field.lower(), api_field.lower())
                    if score >= 70:
                        match_scores.append((score, field, api_field))
            
            match_scores.sort(reverse=True)
            used_fields = set()
            used_api_fields = set()
            
            for score, field, api_field in match_scores:
                if field not in used_fields and api_field not in used_api_fields:
                    self.field_mapping[field] = api_field
                    used_fields.add(field)
                    used_api_fields.add(api_field)
            
            for field in fields_to_match:
                row = self.mapping_table.rowCount()
                self.mapping_table.insertRow(row)
                
                self.mapping_table.setItem(row, 0, QTableWidgetItem(field))
                
                combo = QComboBox()
                combo.addItem("")
                combo.addItems(api_fields)
                
                matched_api_field = self.field_mapping.get(field, "")
                combo.setCurrentText(matched_api_field)
                
                combo.currentTextChanged.connect(lambda text, f=field: self.update_mapping(f, text))
                self.mapping_table.setCellWidget(row, 1, combo)
                
                score = "-"
                if matched_api_field:
                    score = str(max(fuzz.ratio(field.lower(), af.lower()) for af in api_fields if af == matched_api_field))
                self.mapping_table.setItem(row, 2, QTableWidgetItem(score))

            self.mapping_table.resizeColumnsToContents()
            self.submit_button.setEnabled(True)
            self.log_message("Field matching completed")
        except Exception as e:
            self.log_message(f"Error in field matching: {str(e)}")

    def update_mapping(self, field, api_field):
        """Update field mapping when user changes selection."""
        self.field_mapping[field] = api_field if api_field else None
        self.log_message(f"Updated mapping: {field} -> {api_field or 'None'}")

    def submit_features(self):
        """Submit features to API with mapped fields and additional entity data from pcode."""
        try:
            layer = self.layer_combo.currentData()
            url = self.url_input.text()
            entity = self.entity_combo.currentData()
            
            if self.gdf is None or self.gdf.empty:
                self.log_message("No valid GeoDataFrame available for submission.")
                QMessageBox.warning(self, "No Data", "No valid layer selected or layer contains no features.")
                return

            # Disable submit button and show animated progress bar
            self.submit_button.setEnabled(False)
            self.progress_bar.setRange(0, 0)  # Indeterminate mode (animated)
            self.progress_bar.setVisible(True)

            # Drop Z dimension from all geometries in the GeoDataFrame
            try:
                self.gdf['geometry'] = self.gdf.geometry.force_2d()
                self.log_message("Z dimension dropped from all geometries in GeoDataFrame.")
            except Exception as e:
                self.log_message(f"Error dropping Z dimension from geometries: {str(e)}")
                QMessageBox.critical(self, "Geometry Error", f"Failed to drop Z dimension: {str(e)}")
                return

            features = []
            for idx, row in self.gdf.iterrows():
                if idx not in self.valid_feature_indices:
                    self.log_message(f"Skipping feature index {idx}: No valid parent ID found.")
                    continue
                
                feature = {}
                
                if "code" in row and row["code"]:
                    feature["code"] = self._convert_to_serializable(row["code"])
                
                if "pcode" in row and row["pcode"]:
                    feature["pcode"] = self._convert_to_serializable(row["pcode"])
                
                entity_data = self.pcode_entity_data.get(idx, {})
                if entity_data:
                    if entity_data.get("settlement_id") is not None:
                        feature["settlement_id"] = self._convert_to_serializable(entity_data["settlement_id"])
                    if entity_data.get("ward_id") is not None:
                        feature["ward_id"] = self._convert_to_serializable(entity_data["ward_id"])
                    if entity_data.get("subcounty_id") is not None:
                        feature["subcounty_id"] = self._convert_to_serializable(entity_data["subcounty_id"])
                    if entity_data.get("county_id") is not None:
                        feature["county_id"] = self._convert_to_serializable(entity_data["county_id"])
                
                for field, api_field in self.field_mapping.items():
                    if api_field:
                        if field in row:
                            feature[api_field] = self._convert_to_serializable(row[field])
                        elif field in entity_data and entity_data.get(field) is not None:
                            feature[api_field] = self._convert_to_serializable(entity_data.get(field))
                
                if hasattr(row, "geometry") and row.geometry:
                    feature["geom"] = self._convert_to_serializable(row.geometry.__geo_interface__)
                
                features.append(feature)
            
            if not features:
                self.log_message("No features with valid parent IDs to submit.")
                QMessageBox.warning(self, "No Valid Features", "No features with valid parent IDs were found for submission.")
                return

            for i, feature in enumerate(features[:2]):
                self.log_message(f"Feature {i+1}: {json.dumps(feature, indent=2)}")
            
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }
            response = requests.post(
                f"{url}/api/v1/data/import/upsert",
                json={
                    "model": entity["model"],
                    "data": features
                },
                headers=headers
            )

            try:
                response_data = response.json()
                message = response_data.get("message", "Unknown response")
                inserted_count = response_data.get("insertedCount", 0)
                updated_count = response_data.get("updatedCount", 0)
                failed_count = response_data.get("failedCount", 0)
                errors = response_data.get("errors", [])
                
                log_msg = f"{message}: {inserted_count} inserted, {updated_count} updated, {failed_count} failed"
                self.log_message(log_msg)
                
                for idx, error in enumerate(errors):
                    error_item = error.get("item", {})
                    error_code = error_item.get("code", f"Record {idx+1}")
                    error_message = error.get("error", "Unknown error")
                    error_detail = error.get("detail", "No additional details")
                    self.log_message(f"Error for {error_code}: {error_message} - {error_detail}")
                
                detailed_message = (
                    f"{message}\n\n"
                    f"Inserted: {inserted_count}\n"
                    f"Updated: {updated_count}\n"
                    f"Failed: {failed_count}"
                )
                if response.status_code in (200, 207):
                    QMessageBox.information(self, "Import Status", detailed_message)
                else:
                    self.log_message(f"Response errors: {errors}")
                    QMessageBox.critical(self, "Error", detailed_message)
            except ValueError as e:
                self.log_message(f"Failed to parse response: {response.text}")
                QMessageBox.critical(self, "Error", f"Invalid response: {response.text}")
        except Exception as e:
            self.log_message(f"Error submitting features: {str(e)}")
            QMessageBox.critical(self, "Error", str(e))
        finally:
            # Re-enable submit button and hide progress bar
            self.submit_button.setEnabled(True)
            self.progress_bar.setRange(0, 100)  # Reset to percentage mode
            self.progress_bar.setVisible(False)

    def log_message(self, message):
        """Append message to log widget."""
        self.log_textedit.append(message)