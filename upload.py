import subprocess
import sys
import requests
from PyQt5.QtWidgets import (QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, 
                             QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, 
                             QGroupBox, QTextEdit, QScrollArea, QGridLayout, QWidget, QTableWidget, QApplication,
                             QTableWidgetItem, QSizePolicy)
from PyQt5.QtCore import QVariant, QSettings, Qt, QThread, pyqtSignal, QObject
from PyQt5.QtCore import QThread, pyqtSignal, QObject, QTimer
from fuzzywuzzy import fuzz
import json
import geopandas as gpd
import pandas as pd
from qgis.core import QgsVectorLayer, QgsProject
import shortuuid
from shapely.geometry import mapping, shape
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt5.QtWidgets import (QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, 
                             QLineEdit, QComboBox, QHBoxLayout, QMessageBox, QGroupBox, QTextEdit, 
                             QTableWidget, QTableWidgetItem, QSizePolicy)
from PyQt5.QtCore import Qt, QSettings, QThread, pyqtSignal, QObject, QTimer


try:
    from shapely import force_2d
except ImportError:
    def force_2d(geom):
        """Fallback to convert geometry to 2D by dropping Z coordinate."""
        if geom is None:
            return None
        geom_dict = mapping(geom)
        if geom_dict["type"] == "Point":
            geom_dict["coordinates"] = geom_dict["coordinates"][:2]
        elif geom_dict["type"] in ["LineString", "LinearRing"]:
            geom_dict["coordinates"] = [coord[:2] for coord in geom_dict["coordinates"]]
        elif geom_dict["type"] == "Polygon":
            geom_dict["coordinates"] = [[coord[:2] for coord in ring] for ring in geom_dict["coordinates"]]
        elif geom_dict["type"] in ["MultiPoint", "MultiLineString", "MultiPolygon"]:
            geom_dict["coordinates"] = [
                force_2d(shape(sub_geom)).__geo_interface__["coordinates"]
                for sub_geom in geom_dict["coordinates"]
            ]
        return shape(geom_dict)
from rapidfuzz import process, fuzz


class SearchableComboBox(QComboBox):
    """A QComboBox with searchable dropdown list and clearable selection."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.setInsertPolicy(QComboBox.NoInsert)
        self.setFocusPolicy(Qt.StrongFocus)  # Allow keyboard input
        self.lineEdit().textEdited.connect(self.filter_items)
        self.lineEdit().textChanged.connect(self.handle_text_changed)
        self._all_items = []
        self.setMinimumWidth(200)
        self.setPlaceholderText("Search...")

    def addItems(self, items):
        """Store all items for filtering, with '-' as the clear option."""
        self._all_items = ["-"] + items
        super().addItems(self._all_items)

    def filter_items(self, text):
        """Filter dropdown items based on user input in the combo box."""
        self.blockSignals(True)
        self.clear()
        if not text:
            self.addItems(self._all_items)
        else:
            filtered = [item for item in self._all_items if text.lower() in item.lower()]
            self.addItems(filtered)
        self.blockSignals(False)
        if text:  # Only show popup if user is typing
            self.showPopup()

    def handle_text_changed(self, text):
        """Ensure clearing text selects the '-' option."""
        if not text:
            self.setCurrentIndex(0)  # Select '-' reliably

    def setCurrentText(self, text):
        """Ensure the text is set correctly, mapping '-' or empty to no selection."""
        self.blockSignals(True)  # Prevent signal emission during programmatic update
        if text == "-" or not text:
            super().setCurrentIndex(0)  # Select first item ('-')
        else:
            super().setCurrentText(text)
            if text not in self._all_items and text != "-":
                self.addItem(text)
                super().setCurrentText(text)
        self.blockSignals(False)


class Worker(QObject):
    """Worker object to run fetch_pcode_data in a background thread."""
    progress = pyqtSignal(int)
    log = pyqtSignal(str)
    finished = pyqtSignal()
    result = pyqtSignal(dict, list)

    def __init__(self, layer, parent_entity_name, url, token):
        super().__init__()
        self.layer = layer
        self.parent_entity_name = parent_entity_name
        self.url = url
        self.token = token
        self.gdf = None
        self._is_running = True  # Flag to control execution

    def stop(self):
        """Signal the worker to stop execution."""
        self._is_running = False

 


    def run(self):
        """Fetch pcode-based entity data in the background with concurrent batch processing."""
        try:
            if not self._is_running:
                self.log.emit("Worker stopped before starting.")
                self.result.emit({}, [])
                self.finished.emit()
                return

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
            lock = threading.Lock()  # For thread-safe updates
            batch_size = 500
            max_workers = 3  # Limit concurrent threads to avoid server overload
            processed_items = 0

            def process_geometry_batch(start, batch_geometries, batch_num, total_items):
                if not self._is_running:
                    return 0, []
                try:
                    self.log.emit(f"Processing geometry batch {batch_num} ({start+1}–{min(start+batch_size, total_items)} of {total_items})")
                    response = requests.post(
                        f"{self.url}/api/v1/data/intersect",
                        headers=headers,
                        json={
                            "model": self.parent_entity_name,
                            "geometry": batch_geometries,
                            "srid": srid
                        }
                    )
                    if response.status_code == 200:
                        entity_data = response.json()
                        results = entity_data.get("data", [])
                        self.log.emit(f"Received {entity_data.get('count', 0)} intersecting records in batch {batch_num}")
                        batch_results = []
                        for result in results:
                            geometry_index = result.get("geometry_index")
                            records = result.get("records", [])
                            if geometry_index is not None and geometry_index < len(batch_geometries):
                                global_index = start + geometry_index
                                if global_index in index_to_row:
                                    row_idx = index_to_row[global_index]
                                    if records:
                                        record = records[0]
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
                                                data = {
                                                    id_key: record.get("id"),
                                                    **({"settlement_id": record.get("settlement_id")} if id_key != "settlement_id" else {}),
                                                    **({"ward_id": record.get("ward_id")} if id_key != "ward_id" else {}),
                                                    **({"subcounty_id": record.get("subcounty_id")} if id_key != "subcounty_id" else {}),
                                                    **({"county_id": record.get("county_id")} if id_key != "county_id" else {})
                                                }
                                                batch_results.append((row_idx, data))
                                                self.log.emit(f"Assigned {parent_entity_lower}-based data for index {row_idx}: {data}")
                                            else:
                                                self.log.emit(f"Error: Invalid parent entity '{self.parent_entity_name}' for index {row_idx}")
                                        else:
                                            self.log.emit(f"No intersect result for geometry at index {row_idx}")
                                    else:
                                        self.log.emit(f"Invalid global geometry_index {global_index} in response")
                                else:
                                    self.log.emit(f"Invalid geometry_index {geometry_index} in batch {batch_num}")
                        return len(batch_geometries), batch_results
                    else:
                        self.log.emit(f"Failed to fetch batch {batch_num} geometry data: {response.text}")
                        return len(batch_geometries), []
                except Exception as e:
                    self.log.emit(f"Error fetching batch {batch_num} geometry data: {str(e)}")
                    return len(batch_geometries), []

            def process_pcode_batch(start, batch_indices, batch_codes, batch_num, total_items):
                if not self._is_running:
                    return 0, []
                try:
                    self.log.emit(f"Processing pcode batch {batch_num} ({start+1}–{min(start+batch_size, total_items)} of {total_items})")
                    response = requests.post(
                        f"{self.url}/api/v1/data/many/code",
                        headers=headers,
                        json={
                            "model": self.parent_entity_name,
                            "codes": batch_codes
                        }
                    )
                    if response.status_code == 200:
                        payload = response.json()
                        records = payload.get("data", [])
                        code_map = {r["code"]: r for r in records}
                        batch_results = []
                        for row_idx, pcode in batch_indices:
                            rec = code_map.get(pcode)
                            if rec and rec.get("id"):
                                parent = self.parent_entity_name.lower()
                                if parent == "settlement":
                                    key = "settlement_id"
                                elif parent == "ward":
                                    key = "ward_id"
                                elif parent == "subcounty":
                                    key = "subcounty_id"
                                elif parent == "county":
                                    key = "county_id"
                                else:
                                    key = None
                                if key:
                                    data = {
                                        key: rec["id"],
                                        **({"settlement_id": rec.get("settlement_id")} if key != "settlement_id" else {}),
                                        **({"ward_id": rec.get("ward_id")} if key != "ward_id" else {}),
                                        **({"subcounty_id": rec.get("subcounty_id")} if key != "subcounty_id" else {}),
                                        **({"county_id": rec.get("county_id")} if key != "county_id" else {})
                                    }
                                    batch_results.append((row_idx, data))
                                    self.log.emit(f"Batch-fetched data for index {row_idx}: {data}")
                            else:
                                self.log.emit(f"No data for pcode '{pcode}' at index {row_idx}")
                        return len(batch_codes), batch_results
                    else:
                        self.log.emit(f"Batch {batch_num} request failed: {response.text}")
                        return len(batch_codes), []
                except Exception as e:
                    self.log.emit(f"Batch {batch_num} error: {str(e)}")
                    return len(batch_codes), []

            if use_geometry_lookup:
                geometries = [row["geojson"] for _, row in self.gdf.iterrows() if row["geojson"] is not None]
                if not geometries:
                    self.log.emit("No valid geometries found for intersection.")
                    self.result.emit(pcode_entity_data, valid_feature_indices)
                    self.finished.emit()
                    return

                total_items = len(geometries)
                index_to_row = {i: row_idx for i, (row_idx, _) in enumerate(self.gdf.iterrows()) if self.gdf.loc[row_idx, "geojson"] is not None}

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for start in range(0, total_items, batch_size):
                        batch_geometries = geometries[start:start + batch_size]
                        batch_num = start // batch_size + 1
                        futures.append(executor.submit(process_geometry_batch, start, batch_geometries, batch_num, total_items))

                    for future in as_completed(futures):
                        if not self._is_running:
                            self.log.emit("Worker stopped during geometry processing.")
                            break
                        processed, batch_results = future.result()
                        with lock:
                            processed_items += processed
                            for row_idx, data in batch_results:
                                pcode_entity_data[row_idx] = data
                                valid_feature_indices.append(row_idx)
                            progress = int((processed_items / total_items) * 100)
                            self.progress.emit(progress)

            else:
                index_to_pcode = [
                    (row_idx, row["pcode"])
                    for row_idx, row in self.gdf.iterrows()
                    if row.get("pcode")
                ]

                if not index_to_pcode:
                    self.log.emit("No pcode values found; skipping batch lookup.")
                else:
                    total_items = len(index_to_pcode)
                    codes = [p for _, p in index_to_pcode]

                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = []
                        for start in range(0, total_items, batch_size):
                            batch_indices = index_to_pcode[start:start + batch_size]
                            batch_codes = [p for _, p in batch_indices]
                            batch_num = start // batch_size + 1
                            futures.append(executor.submit(process_pcode_batch, start, batch_indices, batch_codes, batch_num, total_items))

                        for future in as_completed(futures):
                            if not self._is_running:
                                self.log.emit("Worker stopped during pcode processing.")
                                break
                            processed, batch_results = future.result()
                            with lock:
                                processed_items += processed
                                for row_idx, data in batch_results:
                                    pcode_entity_data[row_idx] = data
                                    valid_feature_indices.append(row_idx)
                                progress = int((processed_items / total_items) * 100)
                                self.progress.emit(progress)

            if pcode_entity_data:
                self.log.emit(f"Data fetched successfully for {len(valid_feature_indices)} rows with parent entity '{self.parent_entity_name}'.")
            else:
                self.log.emit(f"No data fetched for parent entity '{self.parent_entity_name}'.")

            self.result.emit(pcode_entity_data, valid_feature_indices)
            self.finished.emit()

        except Exception as e:
            self.log.emit(f"Error fetching pcode data: {str(e)}")
            self.finished.emit()
    





class FieldMatchingWorker(QObject):
    """Worker object to run field matching in a background thread."""
    progress = pyqtSignal(int)  # Emit progress percentage
    log = pyqtSignal(str)  # Emit log messages
    finished = pyqtSignal()  # Signal when done
    result = pyqtSignal(dict, list)  # Emit field_mapping and table_data

    def __init__(self, layer, entity, pcode_fields):
        super().__init__()
        self.layer = layer
        self.entity = entity
        self.pcode_fields = pcode_fields
    def stop(self):
            """Signal the worker to stop execution."""
            self._is_running = False
            self.log.emit("Field matching worker stopped.")
            
    def run(self):
        """Perform field matching in the background, picking the best match per API field."""
        try:
            layer_fields    = [f.name() for f in self.layer.fields()]
            fields_to_match = layer_fields + self.pcode_fields
            api_fields      = [attr["name"] for attr in self.entity.get("attributes", [])]
            field_mapping   = {}
            table_data      = []
            scores          = {}  # store scores by layer field

            total = len(fields_to_match)
            # 1) Initial best‐of‐all matching
            for idx, field in enumerate(fields_to_match):
                candidates = process.extract(
                    field,
                    api_fields,
                    scorer=fuzz.ratio,
                    score_cutoff=70
                )
                if candidates:
                    best_field, best_score, _ = max(candidates, key=lambda x: x[1])
                    field_mapping[field] = best_field
                    scores[field] = best_score
                    table_data.append([field, best_field, str(int(best_score))])
                else:
                    field_mapping[field] = None
                    scores[field] = 0
                    table_data.append([field, "", "-"])

                # progress update
                percent = int((idx + 1) / total * 100)
                self.progress.emit(min(percent, 99))
                QThread.msleep(50)

            # 2) Enforce unique API-field assignments
            reverse_map = {}
            for lf, af in field_mapping.items():
                if af:
                    reverse_map.setdefault(af, []).append(lf)

            for api_field, layer_list in reverse_map.items():
                if len(layer_list) > 1:
                    # sort by descending score, keep the top one
                    sorted_layers = sorted(layer_list, key=lambda lf: scores[lf], reverse=True)
                    for duplicate in sorted_layers[1:]:
                        field_mapping[duplicate] = None
                        # clear its entry in table_data
                        for row in table_data:
                            if row[0] == duplicate:
                                row[1] = ""
                                row[2] = "-"
                                break

            # 3) Emit results
            self.log.emit("Field matching completed in background thread.")
            # convert each row back to tuple for the table
            final_table = [(row[0], row[1], row[2]) for row in table_data]
            self.result.emit(field_mapping, final_table)
            self.progress.emit(100)
            self.finished.emit()

        except Exception as e:
            self.log.emit(f"Error in field matching: {e}")
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
        self.pcode_fields = ["settlement_id", "ward_id", "subcounty_id", "county_id"]
        self.is_logged_in = False
        self.settings = QSettings("YourOrganization", "KesMIS")
        self.valid_feature_indices = []
        self.gdf = None
        self._full_table_data = []

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        # Horizontal layout for Server Login and Layer/Parent Selection
        top_layout = QHBoxLayout()

        # Server Login Section
        login_box = QGroupBox("Server Login")
        login_layout = QGridLayout()
        self.url_input = QLineEdit("http://localhost")
        self.username_input = QLineEdit(self.settings.value("username", ""))
        self.password_input = QLineEdit(self.settings.value("password", ""))
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

        # Layer and Parent Selection
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
        self.parent_combo.setEnabled(False)
        self.parent_combo.currentTextChanged.connect(self.start_fetch_pcode_data)
        parent_selection_layout.addWidget(QLabel("Select Parent Entity:"))
        parent_selection_layout.addWidget(self.parent_combo)
        entity_selection_layout = QHBoxLayout()
        self.entity_combo = SearchableComboBox()
        self.entity_combo.setEnabled(False)
        self.entity_combo.currentTextChanged.connect(self.match_fields)
        self.entity_combo.setPlaceholderText("Search entities...")
        entity_selection_layout.addWidget(QLabel("Select Entity:"))
        entity_selection_layout.addWidget(self.entity_combo)
        layer_layout.addLayout(layer_selection_layout)
        layer_layout.addLayout(parent_selection_layout)
        layer_layout.addLayout(entity_selection_layout)
        layer_box.setLayout(layer_layout)
        layer_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        top_layout.addWidget(layer_box, 1)

        # Field Mapping Table
        mapping_box = QGroupBox("Field Mapping")
        mapping_layout = QVBoxLayout()

        # Search bar and clear button
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search layer or API fields...")
        self.search_input.textChanged.connect(self.filter_table)
        self.clear_search_button = QPushButton("Clear")
        self.clear_search_button.clicked.connect(self.clear_search)
        search_layout.addWidget(QLabel("Filter:"))
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(self.clear_search_button)
        mapping_layout.addLayout(search_layout)

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

        # Log Display
        log_box = QGroupBox("Log")
        log_layout = QVBoxLayout()
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        self.log_textedit.setFixedHeight(100)
        self.log_textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
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
        dialog_layout = QVBoxLayout()
        dialog_layout.addWidget(scroll_area)
        self.setLayout(dialog_layout)

        # Thread for background processing
        self.thread = QThread()
        self.worker = None
        self.field_matching_worker = None

    def clear_search(self):
        """Clear the search input and show all table rows."""
        self.search_input.clear()
        self.filter_table("")  # Trigger filter with empty text

    def filter_table(self, text):
        """Filter table rows based on search text, preserving full API field list in combo boxes."""
        text = text.lower()
        for row in range(self.mapping_table.rowCount()):
            layer_field = self.mapping_table.item(row, 0).text().lower()
            api_field_item = self.mapping_table.cellWidget(row, 1)
            api_field = api_field_item.currentText().lower() if api_field_item and api_field_item.currentText() != "-" else ""
            matches = not text or text in layer_field or text in api_field
            self.mapping_table.setRowHidden(row, not matches)
        self.log_message(f"Filtered table with query: '{text}'" if text else "Cleared table filter")















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
        
        if self.parent_combo.currentText():
            self.start_fetch_pcode_data()




    def start_fetch_pcode_data(self):
        """Start fetching pcode data in a background thread."""
        if not self.layer_combo.currentData() or not self.parent_combo.currentText():
            return

        self.pcode_entity_data = {}
        self.valid_feature_indices = []
        self.log_message(f"Starting pcode data fetch for parent entity '{self.parent_combo.currentText()}'")

        self.layer_combo.setEnabled(False)
        self.parent_combo.setEnabled(False)
        self.entity_combo.setEnabled(False)
        self.submit_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)  # Determinate progress bar for all lookups
        self.progress_bar.setValue(0)

        self.worker = Worker(
            self.layer_combo.currentData(),
            self.parent_combo.currentText(),
            self.url_input.text(),
            self.token
        )
        self.worker.gdf = self.gdf
        self.worker.moveToThread(self.thread)

        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.log.connect(self.log_message)
        self.worker.result.connect(self.on_fetch_pcode_data_finished)
        self.worker.finished.connect(self.on_worker_finished)

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
        self.progress_bar.setRange(0, 100)
        self.thread.quit()
        self.thread.wait()
        self.worker = None

    def login_to_server(self):
        """Login to the server and get token with an indeterminate progress bar."""
        try:
            if hasattr(self, '_login_in_progress') and self._login_in_progress:
                self.log_message("Login already in progress. Please wait.")
                QMessageBox.warning(self, "Login In Progress", "A login attempt is already in progress. Please wait.")
                return

            self._login_in_progress = True
            self.log_message("Initiating login...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # Set indeterminate mode
            self.login_button.setEnabled(False)  # Disable login button
            QApplication.processEvents()  # Ensure UI updates immediately

            url = self.url_input.text().rstrip('/')
            username = self.username_input.text()
            password = self.password_input.text()

            # Validate inputs
            if not url or not username or not password:
                self.log_message("Login failed: URL, username, and password are required.")
                QMessageBox.critical(self, "Input Error", "Please provide server URL, username, and password.")
                return

            if not url.startswith(("http://", "https://")):
                self.log_message("Login failed: Invalid server URL format.")
                QMessageBox.critical(self, "Input Error", "Server URL must start with http:// or https://")
                return

            self.log_message("Contacting server...")
            login_url = f"{url}/api/auth/signin"
            try:
                response = requests.post(
                    login_url,
                    json={"username": username, "password": password},
                    timeout=10  # 10-second timeout
                )
                response.raise_for_status()  # Raise exception for bad status codes
            except requests.Timeout:
                self.log_message("Login failed: Server did not respond within 10 seconds.")
                QMessageBox.critical(self, "Timeout Error", "The server did not respond. Please check the URL and try again.")
                return
            except requests.ConnectionError:
                self.log_message("Login failed: Could not connect to the server.")
                QMessageBox.critical(self, "Connection Error", "Could not connect to the server. Please check your network and URL.")
                return
            except requests.HTTPError as e:
                self.log_message(f"Login failed: Server error - {str(e)}")
                QMessageBox.critical(self, "Server Error", f"Server returned an error: {str(e)}")
                return

            self.log_message("Validating credentials...")
            if response.status_code == 200:
                data = response.json()
                self.token = data.get("data")
                if not self.token:
                    self.log_message("Login failed: No token received from server.")
                    QMessageBox.critical(self, "Login Error", "Login failed: No token received from server.")
                    return

                self.is_logged_in = True
                self.log_message(f"Login successful for user '{username}'")

                # Save credentials if checked
                if self.save_credentials.isChecked():
                    self.save_credentials_to_settings()

                # Update UI
                self.log_message("Populating layers...")
                self.populate_layers()
                self.log_message("Fetching entities...")
                self.fetch_entities(url)
                self.layer_combo.setEnabled(True)
                self.parent_combo.setEnabled(True)
                self.entity_combo.setEnabled(True)
                QMessageBox.information(self, "Success", "Logged in successfully!")
            else:
                self.is_logged_in = False
                self.log_message(f"Login failed: {response.text}")
                QMessageBox.critical(self, "Login Error", f"Login failed: {response.text}")

        except Exception as e:
            self.is_logged_in = False
            self.log_message(f"Unexpected login error: {str(e)}")
            QMessageBox.critical(self, "Error", f"An unexpected error occurred: {str(e)}")

        finally:
            self._login_in_progress = False
            self.progress_bar.setRange(0, 100)  # Reset to determinate mode
            self.progress_bar.setValue(0)
            self.progress_bar.setVisible(False)
            self.login_button.setEnabled(True)

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
        """Fetch entities from API and populate the entity combo box."""
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
                entity_names = [entity["model"] for entity in self.api_entities]
                self.entity_combo.addItems(entity_names)
                for i, entity in enumerate(self.api_entities):
                    self.entity_combo.setItemData(i + 1, entity)  # Offset by 1 for '-'
                self.entity_combo.setCurrentIndex(0)  # Select '-' initially
                self.entity_combo.setEnabled(True)
                self.log_message("Entities fetched successfully")
            else:
                self.log_message(f"Failed to fetch entities: {response.text}")
        except Exception as e:
            self.log_message(f"Error fetching entities: {str(e)}")

  
    def match_fields(self):
        """Start field matching in a background thread."""
        try:
            if not self.layer_combo.currentData() or not self.entity_combo.currentData():
                self.log_message("No layer or entity selected for field matching.")
                return

            layer = self.layer_combo.currentData()
            entity = self.entity_combo.currentData()

            self.layer_combo.setEnabled(False)
            self.parent_combo.setEnabled(False)
            self.entity_combo.setEnabled(False)
            self.submit_button.setEnabled(False)
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)

            # Ensure previous worker is cleaned up
            if self.field_matching_worker:
                self.field_matching_worker.deleteLater()
                self.thread.quit()
                self.thread.wait()

            self.field_matching_worker = FieldMatchingWorker(
                layer,
                entity,
                self.pcode_fields
            )
            self.field_matching_worker.moveToThread(self.thread)

            self.field_matching_worker.progress.connect(self.progress_bar.setValue)
            self.field_matching_worker.log.connect(self.log_message)
            self.field_matching_worker.result.connect(self.on_field_matching_finished)
            self.field_matching_worker.finished.connect(self.on_field_matching_worker_finished)

            self.thread.started.connect(self.field_matching_worker.run)
            self.thread.start()

        except Exception as e:
            self.log_message(f"Error starting field matching: {str(e)}")
            self.on_field_matching_worker_finished()

    def on_field_matching_finished(self, field_mapping, table_data):
        """Handle results from field matching worker."""
        self.field_mapping = field_mapping
        self._full_table_data = table_data
        api_fields = [attr["name"] for attr in self.entity_combo.currentData().get("attributes", [])]

        self.mapping_table.blockSignals(True)
        self.mapping_table.setRowCount(0)
        for field, matched_api_field, score in table_data:
            row = self.mapping_table.rowCount()
            self.mapping_table.insertRow(row)
            self.mapping_table.setItem(row, 0, QTableWidgetItem(field))

            combo = SearchableComboBox()
            combo.addItems(api_fields)  # Add full API field list
            combo.setCurrentText(matched_api_field if matched_api_field else "-")
            combo.currentTextChanged.connect(lambda text, f=field: self.update_mapping(f, text))
            self.mapping_table.setCellWidget(row, 1, combo)

            self.mapping_table.setItem(row, 2, QTableWidgetItem(score))

        self.mapping_table.blockSignals(False)
        self.mapping_table.resizeColumnsToContents()
        self.submit_button.setEnabled(True)
        self.log_message("Field mapping table updated.")
        QTimer.singleShot(100, lambda: self.progress_bar.setValue(100))
        self.filter_table(self.search_input.text())



    def on_field_matching_worker_finished(self):
        """Clean up after field matching worker finishes."""
        self.layer_combo.setEnabled(True)
        self.parent_combo.setEnabled(True)
        self.entity_combo.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.progress_bar.setValue(0)
        self.thread.quit()
        self.thread.wait()
        self.field_matching_worker = None

    def update_mapping(self, field, api_field):
        """Update field mapping when user changes selection."""
        self.field_mapping[field] = None if api_field == "-" or not api_field else api_field
        self.log_message(f"Updated mapping: {field} -> {api_field if api_field != '-' else 'None'}")
        for row, (layer_field, _, score) in enumerate(self._full_table_data):
            if layer_field == field:
                self._full_table_data[row] = (layer_field, api_field if api_field != "-" else "", score)
                break


    @staticmethod
    def to_2d(geom):
        """Convert geometry to 2D by dropping Z dimension."""
        if geom is None:
            return None
        return shape(mapping(force_2d(geom)))

    def sanitize_json_value(self, value):
        """Sanitize JSON values to handle NaN and infinity."""
        if isinstance(value, float) and (value != value or value in [float("inf"), float("-inf")]):
            return None
        if value is None:
            return None
        return value

 
    def submit_features(self):
        """Submit features to API in batches of 100 with progress updates."""
        try:
            layer = self.layer_combo.currentData()
            url = self.url_input.text()
            entity = self.entity_combo.currentData()

            # Validate GeoDataFrame
            if self.gdf is None or self.gdf.empty:
                self.log_message("No valid GeoDataFrame available for submission.")
                QMessageBox.warning(self, "No Data", "No valid layer selected or layer contains no features.")
                return

            # Disable UI and prepare progress bar
            self.submit_button.setEnabled(False)
            # Process CRS and geometries
            try:
                srid = layer.crs().postgisSrid()
                if self.gdf.crs is None:
                    self.gdf.set_crs(epsg=srid, inplace=True)
                    self.log_message("No CRS defined for GeoDataFrame. Using layer CRS.")
                self.gdf = self.gdf.to_crs(epsg=4326)
                self.gdf['geometry'] = self.gdf['geometry'].apply(self.to_2d)
                self.log_message("Z dimension dropped and GeoDataFrame reprojected to EPSG:4326.")
            except Exception as e:
                self.log_message(f"Error reprojecting or processing geometries: {e}")
                QMessageBox.critical(self, "Geometry Error", f"Failed to reproject or process geometries: {e}")
                return

            # Build feature payloads
            features = []
            for idx, row in self.gdf.iterrows():
                if idx not in self.valid_feature_indices:
                    self.log_message(f"Skipping feature index {idx}: No valid parent ID found.")
                    continue

                feature = {}
                # include codes and pcode if present
                for key in ("code", "pcode"):  # optional keys
                    if key in row and row[key]:
                        feature[key] = self._convert_to_serializable(row[key])

                # parent entity IDs
                entity_data = self.pcode_entity_data.get(idx, {})
                for id_key in ("settlement_id", "ward_id", "subcounty_id", "county_id"):
                    if entity_data.get(id_key) is not None:
                        feature[id_key] = entity_data[id_key]

                # mapped fields
                for field, api_field in self.field_mapping.items():
                    if not api_field:
                        continue
                    if field in row:
                        feature[api_field] = self.sanitize_json_value(row[field])
                    elif field in entity_data:
                        feature[api_field] = self.sanitize_json_value(entity_data[field])

                # geometry
                if hasattr(row, "geometry") and row.geometry:
                    feature["geom"] = row.geometry.__geo_interface__
                features.append(feature)

            if not features:
                self.log_message("No features with valid parent IDs to submit.")
                QMessageBox.warning(self, "No Valid Features", "No features with valid parent IDs were found for submission.")
                return

            # Batch submission setup
            batch_size = 100
            total = len(features)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_bar.setVisible(True)

            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }
            all_inserted = all_updated = all_failed = 0
            all_errors = []

            # Submit in batches
            for start in range(0, total, batch_size):
                batch = features[start:start + batch_size]
                batch_num = start // batch_size + 1
                self.log_message(f"Submitting batch {batch_num} ({start+1}–{min(start+batch_size, total)} of {total})...")
                try:
                    resp = requests.post(
                        f"{url}/api/v1/data/import/upsert",
                        json={"model": entity["model"], "data": batch},
                        headers=headers
                    )
                    data = resp.json()
                    all_inserted += data.get("insertedCount", 0)
                    all_updated += data.get("updatedCount", 0)
                    all_failed += data.get("failedCount", 0)
                    all_errors.extend(data.get("errors", []))
                except Exception as e:
                    self.log_message(f"Batch {batch_num} failed entirely: {e}")
                    all_failed += len(batch)

                # Update progress
                percent = int((start + len(batch)) / total * 100)
                self.progress_bar.setValue(percent)

            # Finalize
            self.progress_bar.setValue(100)
            self.progress_bar.setVisible(False)

            summary = f"Done: {all_inserted} inserted, {all_updated} updated, {all_failed} failed."
            self.log_message(summary)
            for err in all_errors:
                code = err.get("item", {}).get("code", "<unknown>")
                self.log_message(f"Error {code}: {err.get('error')} — {err.get('detail')}")
            QMessageBox.information(self, "Import Complete", summary)

        except Exception as e:
            self.log_message(f"Error submitting features: {e}")
            QMessageBox.critical(self, "Error", str(e))

        finally:
            self.submit_button.setEnabled(True)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setVisible(False)

    def log_message(self, message):
        """Append message to log widget."""
        self.log_textedit.append(message)

    def closeEvent(self, event):
        """Handle dialog close event to clean up threads and workers."""
        if self.thread.isRunning():
            if self.worker:
                self.worker.stop()
                self.worker.deleteLater()
            if self.field_matching_worker:
                self.field_matching_worker.stop()
                self.field_matching_worker.deleteLater()
            self.thread.quit()
            self.thread.wait()
        event.accept()