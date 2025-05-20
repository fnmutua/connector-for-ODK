import subprocess
import sys
import requests
from PyQt5.QtWidgets import QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, QGroupBox, QTextEdit, QScrollArea, QGridLayout, QWidget, QTableWidget, QTableWidgetItem
from PyQt5.QtCore import QVariant, QSettings
from fuzzywuzzy import fuzz
import json
import geopandas as gpd
import pandas as pd
from qgis.core import QgsVectorLayer, QgsProject

class KesMISDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Export data to KeSMIS")
        self.setFixedSize(1000, 800)

        # Initialize variables
        self.token = None
        self.api_entities = []
        self.field_mapping = {}
        self.pcode_entity_data = {}  # Store pcode-based entity data
        self.pcode_fields = []  # Store pcode-derived fields for matching
        self.is_logged_in = False  # Track login state
        self.settings = QSettings("YourOrganization", "KesMIS")  # Initialize QSettings
        
        # Layout
        layout = QVBoxLayout()

        # Server Login Section
        login_box = QGroupBox("Server Login")
        login_layout = QGridLayout()
        
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        
        self.url_input = QLineEdit()
        self.url_input.setText("http://localhost")  # Default URL, not preloaded from QSettings
        self.url_input.setPlaceholderText("Enter server URL")
        self.username_input = QLineEdit()
        self.username_input.setText(self.settings.value("username", ""))  # Preload username
        self.username_input.setPlaceholderText("Username")
        self.password_input = QLineEdit()
        self.password_input.setText(self.settings.value("password", ""))  # Preload password
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

        # Layer and Parent Selection
        layer_box = QGroupBox("Layer and Parent Selection")
        layer_layout = QVBoxLayout()
        
        # Layer selection
        layer_selection_layout = QHBoxLayout()
        self.layer_combo = QComboBox()
        self.layer_combo.setEnabled(False)
        layer_selection_layout.addWidget(QLabel("Select Layer:"))
        layer_selection_layout.addWidget(self.layer_combo)
        
        # Parent entity selection
        parent_selection_layout = QHBoxLayout()
        self.parent_combo = QComboBox()
        self.parent_combo.addItems(["", "settlement", "ward"])  # Add empty option
        self.parent_combo.setCurrentIndex(0)  # No default selection
        self.parent_combo.setEnabled(False)
        self.parent_combo.currentTextChanged.connect(self.fetch_pcode_data)
        parent_selection_layout.addWidget(QLabel("Select Parent Entity:"))
        parent_selection_layout.addWidget(self.parent_combo)
        
        # Entity selection
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

        # Field Mapping Table
        mapping_box = QGroupBox("Field Mapping")
        mapping_layout = QVBoxLayout()
        
        self.mapping_table = QTableWidget()
        self.mapping_table.setColumnCount(3)
        self.mapping_table.setHorizontalHeaderLabels(["Layer Field", "API Field", "Match Score"])
        mapping_layout.addWidget(self.mapping_table)
        
        self.submit_button = QPushButton("Submit Data to KeSMIS")
        self.submit_button.setEnabled(False)
        self.submit_button.clicked.connect(self.submit_features)
        mapping_layout.addWidget(self.submit_button)
        
        mapping_box.setLayout(mapping_layout)

        # Log Display
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)

        # Add to main layout
        layout.addWidget(login_box)
        layout.addWidget(layer_box)
        layout.addWidget(mapping_box)
        layout.addWidget(self.log_textedit)
        
        # Clear Log Button
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)
        layout.addWidget(self.clear_log_button)


        
        self.setLayout(layout)

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
            return str(value)  # Fallback to string representation

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
                self.is_logged_in = True  # Set login state
                self.log_message("Login successful!")
                
                # Save credentials if checkbox is checked
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
        if state == 2 and self.is_logged_in:  # Checked and logged in
            self.save_credentials_to_settings()

    def save_credentials_to_settings(self):
        """Save the entered credentials to QSettings."""
        url = self.url_input.text()
        username = self.username_input.text()
        password = self.password_input.text()

        # Save to QSettings
        self.settings.setValue("url", url)
        self.settings.setValue("username", username)
        self.settings.setValue("password", password)

        # Show a confirmation message
        QMessageBox.information(self, "Success", "Credentials saved successfully!")

        # Log for debugging (avoid logging password in production)
        self.log_message(f"Saved credentials: URL={url}, Username={username}")

    def populate_layers(self):
        """Populate available layers from QGIS canvas."""
        self.layer_combo.clear()
        layers = QgsProject.instance().mapLayers().values()
        for layer in layers:
            if isinstance(layer, QgsVectorLayer):
                self.layer_combo.addItem(layer.name(), layer)
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
                self.entity_combo.addItem("")  # Add empty option
                for entity in self.api_entities:
                    self.entity_combo.addItem(entity["model"], entity)
                self.entity_combo.setCurrentIndex(0)  # No default selection
                self.entity_combo.setEnabled(True)
                self.log_message("Entities fetched successfully")
            else:
                self.log_message(f"Failed to fetch entities: {response.text}")
        except Exception as e:
            self.log_message(f"Error fetching entities: {str(e)}")

    def fetch_pcode_data(self):
        """Fetch pcode-based entity data when parent entity is selected using a POST request."""
        try:
            if not self.layer_combo.currentData() or not self.parent_combo.currentText():
                return

            layer = self.layer_combo.currentData()
            parent_entity_name = self.parent_combo.currentText()

            layer_fields = [f.name() for f in layer.fields()]

            use_geometry_lookup = False
            if "pcode" not in layer_fields:
                reply = QMessageBox.question(
                    self,
                    "Missing Pcode",
                    "The selected layer does not contain a 'pcode' field. Would you like to use geometry to fetch parent entity data?",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No
                )
                if reply == QMessageBox.Yes:
                    use_geometry_lookup = True
                else:
                    self.log_message("Error: 'pcode' field not found and geometry lookup declined.")
                    QMessageBox.critical(self, "Missing Field", "The selected layer must contain a 'pcode' field or allow geometry lookup.")
                    self.submit_button.setEnabled(False)
                    return

            srid = layer.crs().postgisSrid()
            self.log_message(f"Layer CRS SRID detected: {srid}")

            self.pcode_entity_data = {}
            self.pcode_fields = ["settlement_id", "ward_id", "subcounty_id", "county_id"]

            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }
            url = self.url_input.text()

            gdf = gpd.GeoDataFrame.from_features([
                {
                    "type": "Feature",
                    "geometry": json.loads(f.geometry().asJson()) if f.geometry() else None,
                    "properties": {field: self._convert_to_serializable(f[field]) for field in layer_fields}
                } for f in layer.getFeatures()
            ])

            for idx, row in gdf.iterrows():
                if use_geometry_lookup and (not hasattr(row, "geometry") or not row.geometry):
                    self.log_message(f"No geometry for feature index {idx}, skipped.")
                    continue

                pcode = row["pcode"] if "pcode" in row else None
                if pcode and not use_geometry_lookup:
                    try:
                        response = requests.post(
                            f"{url}/api/v1/data/one/code",
                            headers=headers,
                            json={
                                "model": parent_entity_name,
                                "code": pcode
                            }
                        )
                        if response.status_code == 200:
                            entity_data = response.json()
                            record = entity_data.get("data", {})
                            # if record and record.get("id"):
                            #     self.pcode_entity_data[idx] = {
                            #         "settlement_id": record.get("id"),
                            #         "ward_id": record.get("ward_id"),
                            #         "subcounty_id": record.get("subcounty_id"),
                            #         "county_id": record.get("county_id")
                            #     }
                            #     self.log_message(f"Fetched pcode data for index {idx}: {self.pcode_entity_data[idx]}")
                            
                            if record and record.get("id"):
                                id_key = None
                                parent_entity_lower = parent_entity_name.lower()
                                if parent_entity_lower == "settlement":
                                    id_key = "settlement_id"
                                elif parent_entity_lower == "ward":
                                    id_key = "ward_id"
                                elif parent_entity_lower == "subcounty":
                                    id_key = "subcounty_id"
                                elif parent_entity_lower == "county":
                                    id_key = "county_id"

                                if id_key:
                                    self.pcode_entity_data[idx] = {
                                        id_key: record.get("id"),
                                        "settlement_id": record.get("settlement_id"),
                                        "ward_id": record.get("ward_id"),
                                        "subcounty_id": record.get("subcounty_id"),
                                        "county_id": record.get("county_id")
                                    }
                                    self.log_message(f"Fetched data for index {idx}: {self.pcode_entity_data[idx]}")                            
                            
                            
                            else:
                                self.log_message(f"No matching data found for pcode '{pcode}' at index {idx}")
                        else:
                            self.log_message(f"Failed to fetch entity data for pcode {pcode}: {response.text}")
                    except Exception as e:
                        self.log_message(f"Error fetching entity data for pcode {pcode}: {str(e)}")
                
                elif use_geometry_lookup:
                    try:
                        response = requests.post(
                            f"{url}/api/v1/data/intersect",
                            headers=headers,
                            json={
                                "model": parent_entity_name,
                                "geometry": self._convert_to_serializable(row.geometry.__geo_interface__),
                                "srid": srid
                            }
                        )
                        if response.status_code == 200:
                            entity_data = response.json()
                            data_list = entity_data.get("data", [])
                            record = data_list[0] 
                            if record and record.get("id"):
                                self.pcode_entity_data[idx] = {
                                    "settlement_id": record.get("id"),
                                    "ward_id": record.get("ward_id"),
                                    "subcounty_id": record.get("subcounty_id"),
                                    "county_id": record.get("county_id")
                                }
                                self.log_message(f"Fetched geometry-based data for index {idx}: {self.pcode_entity_data[idx]}")
                            else:
                                self.log_message(f"No intersect result for geometry at index {idx}")
                        else:
                            self.log_message(f"Failed to fetch entity data for geometry at index {idx}: {response.text}")
                    except Exception as e:
                        self.log_message(f"Error fetching entity data for geometry at index {idx}: {str(e)}")
                else:
                    self.log_message(f"No pcode for feature index {idx}, skipped.")

            if self.pcode_entity_data:
                self.log_message("Pcode-based entity data fetched successfully.")
            else:
                self.log_message("No entity data could be fetched for any features.")

        except Exception as e:
            self.log_message(f"Error fetching pcode data: {str(e)}")



    def match_fields(self):
        """Perform one-to-one fuzzy matching with minimum 70% score, allowing unmatched fields."""
        try:
            if not self.layer_combo.currentData() or not self.entity_combo.currentData():
                return

            layer = self.layer_combo.currentData()
            entity = self.entity_combo.currentData()
            
            # Get layer fields
            layer_fields = [f.name() for f in layer.fields()]
            
            # Combine layer fields with pcode-derived fields
            fields_to_match = layer_fields + self.pcode_fields
            
            # Get API entity fields from attributes
            api_fields = [attr["name"] for attr in entity.get("attributes", [])]
            
            # Clear existing mappings
            self.field_mapping = {}
            self.mapping_table.setRowCount(0)
            
            # Compute fuzzy matching scores for all pairs, filter for >= 70%
            match_scores = []
            for field in fields_to_match:
                for api_field in api_fields:
                    score = fuzz.ratio(field.lower(), api_field.lower())
                    if score >= 70:
                        match_scores.append((score, field, api_field))
            
            # Sort by score in descending order
            match_scores.sort(reverse=True)
            
            # Track used fields and API fields
            used_fields = set()
            used_api_fields = set()
            
            # Perform one-to-one matching for scores >= 70%
            for score, field, api_field in match_scores:
                if field not in used_fields and api_field not in used_api_fields:
                    self.field_mapping[field] = api_field
                    used_fields.add(field)
                    used_api_fields.add(api_field)
            
            # Add all fields to mapping table, including unmatched ones
            for field in fields_to_match:
                row = self.mapping_table.rowCount()
                self.mapping_table.insertRow(row)
                
                self.mapping_table.setItem(row, 0, QTableWidgetItem(field))
                
                # Create combo box for API fields
                combo = QComboBox()
                combo.addItem("")  # Add empty option for unmatched fields
                combo.addItems(api_fields)
                
                # Set current text to matched API field or empty if unmatched
                matched_api_field = self.field_mapping.get(field, "")
                combo.setCurrentText(matched_api_field)
                
                combo.currentTextChanged.connect(lambda text, f=field: self.update_mapping(f, text))
                self.mapping_table.setCellWidget(row, 1, combo)
                
                # Set score (or "-" for unmatched fields)
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

    def drop_z_dimension(self, geometry):
        """Drop Z dimension from GeoJSON geometry coordinates."""
        if isinstance(geometry, dict):
            if 'coordinates' in geometry:
                geometry['coordinates'] = self.drop_z_dimension(geometry['coordinates'])
            return geometry
        elif isinstance(geometry, list):
            # Handle nested coordinate arrays (e.g., Polygons, Multi* geometries)
            return [self.drop_z_dimension(coord) for coord in geometry]
        elif isinstance(geometry, tuple):
            # Drop Z coordinate, keep X and Y
            return geometry[:2]
        return geometry

    def submit_features(self):
        """Submit features to API with mapped fields and additional entity data from pcode."""
        try:
            layer = self.layer_combo.currentData()
            url = self.url_input.text()
            entity = self.entity_combo.currentData()
            
            # Convert layer features to GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features([
                {
                    "type": "Feature",
                    "geometry": json.loads(f.geometry().asJson()) if f.geometry() else None,
                    "properties": {field: self._convert_to_serializable(f[field]) for field in [f.name() for f in layer.fields()]}
                } for f in layer.getFeatures()
            ])
            
            # Prepare features with mapped fields and entity data
            features = []
            for idx, row in gdf.iterrows():
                feature = {}
                
                # Add pcode as code if available
                if "pcode" in row and row["pcode"]:
                    feature["code"] = self._convert_to_serializable(row["pcode"])
                
                # Add entity data from pcode_entity_data
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
                
                # Add mapped fields
                for field, api_field in self.field_mapping.items():
                    if api_field:  # Only include if a mapping exists
                        if field in row:
                            feature[api_field] = self._convert_to_serializable(row[field])
                        elif field in entity_data and entity_data.get(field) is not None:
                            feature[api_field] = self._convert_to_serializable(entity_data.get(field))
                
                # Handle geometry (convert to GeoJSON and drop Z dimension)
                if hasattr(row, "geometry") and row.geometry:
                    geojson = self.drop_z_dimension(row.geometry.__geo_interface__)
                    feature["geom"] = self._convert_to_serializable(geojson)
                
                features.append(feature)
            
            # Log the first two records
            for i, feature in enumerate(features[:2]):
                self.log_message(f"Feature {i+1}: {json.dumps(feature, indent=2)}")
            
            # Submit to API
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
                
                log_msg = f"{message}: {inserted_count} inserted, {updated_count} updated, {failed_count} failed"
                self.log_message(log_msg)
                
                if response.status_code in (200, 207):
                    QMessageBox.information(self, "Import Status", message)
                else:
                    self.log_message(f"Errors: {response_data.get('errors', [])}")
                    QMessageBox.critical(self, "Error", message)
            except ValueError as e:
                self.log_message(f"Failed to parse response: {response.text}")
                QMessageBox.critical(self, "Error", f"Invalid response: {response.text}")
        except Exception as e:
            self.log_message(f"Error submitting features: {str(e)}")
            QMessageBox.critical(self, "Error", str(e))

    def log_message(self, message):
        """Append message to log widget."""
        self.log_textedit.append(message)