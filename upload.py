import subprocess
import sys
import requests
import os
from datetime import datetime
from PyQt5.QtWidgets import (
    QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox,
    QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox,
    QGroupBox, QTextEdit, QScrollArea, QGridLayout, QWidget, QTableWidget, QApplication,
    QTableWidgetItem, QSizePolicy, QFormLayout, QStackedWidget, QTabWidget,
)
from PyQt5.QtCore import QVariant, QSettings, Qt, QThread, pyqtSignal, QObject, QTimer
from fuzzywuzzy import fuzz
import json
import geopandas as gpd
import pandas as pd
from qgis.core import QgsVectorLayer, QgsProject, QgsDataSourceUri
import shortuuid
from shapely.geometry import mapping, shape
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from rapidfuzz import process, fuzz

from .help_panel import CollapsibleHelpMixin, resize_dialog_to_screen, configure_qgis_dialog
from .code_helper_qgis_console import is_settlement_data_layer, process_layer


def _join_ui_text(*parts):
    """Join static UI label fragments (display only, not SQL)."""
    return "".join(str(part) for part in parts)

def validate_and_repair_geometry(geom, tolerance=1e-8):
    """
    Validate and repair geometry to handle TopologyException errors.
    
    Args:
        geom: Shapely geometry object
        tolerance: Tolerance for coordinate snapping
    
    Returns:
        Repaired geometry or None if repair fails
    """
    if geom is None:
        return None
    
    try:
        # First try to make the geometry valid
        if not geom.is_valid:
            geom = make_valid(geom)
            if geom is None:
                return None
        
        # Apply coordinate snapping to fix precision issues
        if hasattr(geom, 'simplify'):
            geom = geom.simplify(tolerance, preserve_topology=True)
        
        # Final validation check
        if geom.is_valid and not geom.is_empty:
            return geom
        else:
            return None
            
    except Exception as e:
        # If all else fails, try to create a bounding box as fallback
        try:
            if hasattr(geom, 'bounds'):
                minx, miny, maxx, maxy = geom.bounds
                return box(minx, miny, maxx, maxy)
        except:
            pass
        return None

def add_geojson_to_map(filepath, layer_name):
    """
    Add a GeoJSON file as a layer to the QGIS map.
    
    Args:
        filepath: Path to the GeoJSON file
        layer_name: Name for the layer in QGIS
    
    Returns:
        QgsVectorLayer object if successful, None otherwise
    """
    try:
        # Create the layer from the GeoJSON file
        layer = QgsVectorLayer(filepath, layer_name, "ogr")
        
        if layer.isValid():
            # Add the layer to the project
            QgsProject.instance().addMapLayer(layer)
            return layer
        else:
            return None
            
    except Exception as e:
        print(f"Error adding GeoJSON to map: {str(e)}")
        return None

try:
    from shapely import force_2d
    from shapely.validation import make_valid
    from shapely.geometry import box
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
    
    def make_valid(geom):
        """Fallback make_valid function."""
        if geom is None:
            return None
        try:
            # Try to fix common issues
            if hasattr(geom, 'buffer'):
                return geom.buffer(0)
            return geom
        except:
            return None


class SearchableComboBox(QComboBox):
    """A QComboBox with searchable dropdown list and clearable selection."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setEditable(True)
        self.setInsertPolicy(QComboBox.NoInsert)
        self.setFocusPolicy(Qt.StrongFocus)
        self._source_items = []
        self._item_data = {}
        self._updating = False
        self.lineEdit().textEdited.connect(self._on_text_edited)
        self.lineEdit().textChanged.connect(self._on_text_changed)
        self.setMinimumWidth(200)
        self.setPlaceholderText("Search...")

    def addItems(self, items):
        """Store the full item list for filtering, with '-' as the clear option."""
        self._source_items = list(items)
        self._item_data = {}
        self._populate_dropdown(filter_text="", selected_text="-")

    def setItemData(self, index, value, role=Qt.UserRole):
        """Persist item data by text so it survives dropdown rebuilds."""
        text = self.itemText(index)
        if text and text != "-":
            self._item_data[text] = value
        super().setItemData(index, value, role)

    def _apply_item_data(self):
        for i in range(self.count()):
            text = self.itemText(i)
            if text in self._item_data:
                super().setItemData(i, self._item_data[text], Qt.UserRole)

    def _populate_dropdown(self, filter_text="", selected_text=None):
        if selected_text is None:
            selected_text = self.lineEdit().text() if self.lineEdit() else ""

        self._updating = True
        line_edit = self.lineEdit()
        if line_edit:
            line_edit.blockSignals(True)
        self.blockSignals(True)

        super().clear()

        if not filter_text or filter_text == "-":
            choices = ["-"] + list(self._source_items)
        else:
            needle = filter_text.lower()
            matches = [item for item in self._source_items if needle in item.lower()]
            if matches:
                choices = ["-"] + matches
            else:
                choices = ["-", filter_text]

        super().addItems(choices)
        self._apply_item_data()

        if selected_text and selected_text != "-":
            idx = self.findText(selected_text)
            if idx >= 0:
                self.setCurrentIndex(idx)
            else:
                super().addItem(selected_text)
                self.setCurrentIndex(self.count() - 1)
            if line_edit:
                line_edit.setText(selected_text)
        else:
            self.setCurrentIndex(0)
            if line_edit:
                line_edit.setText("")

        if line_edit:
            line_edit.blockSignals(False)
        self.blockSignals(False)
        self._updating = False

    def _on_text_edited(self, text):
        if self._updating:
            return
        if not text:
            self._populate_dropdown(filter_text="", selected_text="-")
            return
        self._populate_dropdown(filter_text=text, selected_text=text)
        self.showPopup()

    def _on_text_changed(self, text):
        if self._updating:
            return
        if not text:
            self._populate_dropdown(filter_text="", selected_text="-")

    def showPopup(self):
        """Open the dropdown with the full option list; current value stays selected."""
        current = self.lineEdit().text().strip() if self.lineEdit() else self.currentText().strip()
        selected = current if current and current != "-" else "-"
        self._populate_dropdown(filter_text="", selected_text=selected)
        super().showPopup()

    def setCurrentText(self, text):
        """Set the current value without losing the stored dropdown options."""
        if text == "-" or not text:
            self._populate_dropdown(filter_text="", selected_text="-")
        else:
            self._populate_dropdown(filter_text="", selected_text=text)


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
        self._is_running = True

    def stop(self):
        """Signal the worker to stop execution."""
        self._is_running = False

    def buffer_geometry(self, geom, distance_km=0.5):
        """Buffer a geometry by a distance in kilometers (approx. for EPSG:4326)."""
        if geom is None:
            return None
        distance_deg = distance_km / 111.0
        return geom.buffer(distance_deg)

    @staticmethod
    def to_2d(geom):
        """Convert geometry to 2D by dropping Z dimension."""
        if geom is None:
            return None
        return shape(mapping(force_2d(geom)))

    def fetch_settlements_geojson(self):
        """Fetch parent GeoJSON from the server or a user-selected local cache file."""
        geojson = None
        downloaded = False
        fallback = getattr(self, 'local_fallback_filepath', None)

        if getattr(self, 'use_local_file', False):
            if not fallback or not os.path.exists(fallback):
                raise FileNotFoundError(
                    f"No local parent GeoJSON file found at {fallback or '(not set)'}"
                )
            self.log.emit(f"Using existing local file selected by user: {fallback}")
            with open(fallback, 'r', encoding='utf-8') as f:
                geojson = json.load(f)
        else:
            headers = {"Authorization": f"Bearer {self.token}", "x-access-token": self.token}
            try:
                self.log.emit(f"Fetching {self.parent_entity_name} GeoJSON from server…")
                response = requests.get(
                    f"{self.url}/api/v1/data/geo/minimal",
                    headers=headers,
                    params={'model': self.parent_entity_name},
                    timeout=30
                )
                response.raise_for_status()
                geojson = response.json()
                downloaded = True
            except Exception as e:
                if fallback and os.path.exists(fallback):
                    self.log.emit(
                        f"Warning: Could not download {self.parent_entity_name} GeoJSON ({e}). "
                        f"Using cached local file: {fallback}"
                    )
                    with open(fallback, 'r', encoding='utf-8') as f:
                        geojson = json.load(f)
                else:
                    msg = f"Error fetching {self.parent_entity_name} GeoJSON: {e}"
                    if fallback:
                        msg += f" No cached local file at {fallback}."
                    self.log.emit(msg)
                    raise

        try:
            if not isinstance(geojson, dict) or "features" not in geojson or geojson.get('type') != 'FeatureCollection':
                raise ValueError("Invalid GeoJSON format")

            if downloaded:
                try:
                    documents_path = os.path.expanduser("~/Documents")
                    odk_data_path = os.path.join(documents_path, "ODK_Data")
                    os.makedirs(odk_data_path, exist_ok=True)

                    filename = f"parent({self.parent_entity_name}).geojson"
                    filepath = getattr(self, 'local_fallback_filepath', None) or os.path.join(
                        odk_data_path, filename
                    )

                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(geojson, f, indent=2, ensure_ascii=False)

                    self.log.emit(f"Saved {self.parent_entity_name} GeoJSON to: {filepath}")

                    layer_name = f"{self.parent_entity_name.capitalize()} Boundaries"
                    layer = add_geojson_to_map(filepath, layer_name)
                    if layer:
                        self.log.emit(f"Added {layer_name} layer to QGIS map")
                    else:
                        self.log.emit(f"Warning: Could not add {layer_name} layer to map")

                except Exception as save_error:
                    self.log.emit(f"Warning: Could not save GeoJSON file: {str(save_error)}")

            # Safely build GeoDataFrame, skipping/repairing invalid geometries that can
            # cause "A linearring requires at least 4 coordinates" errors.
            raw_features = geojson["features"]
            safe_records = []
            skipped = 0

            for feat in raw_features:
                geom_dict = feat.get("geometry")
                if not geom_dict:
                    skipped += 1
                    continue

                try:
                    geom = shape(geom_dict)
                except Exception:
                    # Geometry cannot even be constructed → skip
                    skipped += 1
                    continue

                geom = validate_and_repair_geometry(geom)
                if geom is None or geom.is_empty:
                    skipped += 1
                    continue

                # Collect properties; fall back to everything except geometry/type
                props = feat.get("properties")
                if props is None:
                    props = {k: v for k, v in feat.items() if k not in ("geometry", "type")}

                record = dict(props)
                record["geometry"] = geom
                safe_records.append(record)

            if not safe_records:
                raise ValueError("No valid settlement geometries found after cleaning.")

            settlements_gdf = gpd.GeoDataFrame(safe_records, geometry="geometry", crs="EPSG:4326")

            # 1) Immediately force CRS to EPSG:4326 (WGS84) so intersection logic in 4326 works correctly
            settlements_gdf.set_crs(epsg=4326, inplace=True)

            loaded_count = len(settlements_gdf)
            self.log.emit(
                f"Loaded {loaded_count} settlement features (CRS={settlements_gdf.crs}). "
                f"Skipped {skipped} invalid/empty geometries."
            )
            return settlements_gdf

        except Exception as e:
            self.log.emit(f"Error fetching {self.parent_entity_name} GeoJSON: {str(e)}")
            raise

    def process_pcode_batch(self, start, batch_indices, batch_codes, batch_num, total_items, batch_size):
        """Process a batch of pcode-based queries."""
        if not self._is_running:
            return 0, []
        try:
            self.log.emit(
                f"Processing pcode batch {batch_num} "
                f"({start+1}–{min(start+batch_size, total_items)} of {total_items})"
            )
            response = requests.post(
                f"{self.url}/api/v1/data/many/code",
                headers={"Authorization": f"Bearer {self.token}", "x-access-token": self.token},
                json={"model": self.parent_entity_name, "codes": batch_codes},
                timeout=30
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
                                key: int(rec["id"]) if rec.get("id") is not None else None,
                                **({"settlement_id": int(rec.get("settlement_id")) if rec.get("settlement_id") is not None else None} if key != "settlement_id" else {}),
                                **({"ward_id": int(rec.get("ward_id")) if rec.get("ward_id") is not None else None} if key != "ward_id" else {}),
                                **({"subcounty_id": int(rec.get("subcounty_id")) if rec.get("subcounty_id") is not None else None} if key != "subcounty_id" else {}),
                                **({"county_id": int(rec.get("county_id")) if rec.get("county_id") is not None else None} if key != "county_id" else {})
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

    def run(self):
        """
        Fetch entity data with local intersection first, then pcode matching for unmatched rows:
        - Uses GeoDataFrame already in EPSG:4326 from reset_data()
        - For Points: buffer 1 km (≈0.009°) and do intersects normally.
        - For Polygons: compute exact intersection areas and pick the settlement with the greatest overlap.
        - For unmatched rows: fallback to pcode matching if available.
        """
        try:
            if not self._is_running:
                self.log.emit("Worker stopped before starting.")
                self.result.emit({}, [])
                self.finished.emit()
                return

            # 1) Ensure self.gdf exists
            if self.gdf is None:
                self.log.emit("No GeoDataFrame available; aborting.")
                self.result.emit({}, [])
                self.finished.emit()
                return

            # 2) Verify we're in EPSG:4326
            if self.gdf.crs is None or self.gdf.crs.to_epsg() != 4326:
                self.log.emit("Error: GeoDataFrame must be in EPSG:4326")
                self.result.emit({}, [])
                self.finished.emit()
                return

            # 3) Check for "pcode" column
            layer_fields = [f.name() for f in self.layer.fields()]
            has_pcode = "pcode" in layer_fields
            self.log.emit(f"Pcode column {'found' if has_pcode else 'not found'} in layer.")

            pcode_entity_data = {}
            valid_feature_indices = []
            lock = threading.Lock()
            batch_size = 500
            max_workers = 3
            processed_items = 0
            total_items = len(self.gdf)

            # ── STEP 1: Local intersection for all rows with geometry ─────────────
            # 1.1: Build a list of indices where geometry is non‐null
            intersection_indices = [
                row_idx
                for row_idx, _ in self.gdf.iterrows()
                if self.gdf.loc[row_idx, "geojson"] is not None
            ]
            self.log.emit(f"Processing {len(intersection_indices)} rows with local intersection.")

            if intersection_indices:
                # 1.2: Fetch settlements (already forced to EPSG:4326 in fetch_settlements_geojson())
                settlements_gdf = self.fetch_settlements_geojson()

                # 1.3: Build a GeoDataFrame of all features for intersection
                unmatched_gdf_full = self.gdf.loc[intersection_indices].copy()
                unmatched_gdf_full["geometry"] = unmatched_gdf_full["geojson"].apply(
                    lambda x: shape(x) if x else None
                )
                unmatched_gdf_full = unmatched_gdf_full[
                    unmatched_gdf_full["geometry"].notnull()
                ].reset_index(drop=False)

                # 1.4: Split into points vs polygons vs lines
                point_mask = unmatched_gdf_full.geometry.geom_type.isin(["Point", "MultiPoint"])
                polygon_mask = unmatched_gdf_full.geometry.geom_type.isin(["Polygon",  "MultiPolygon"])
                line_mask    = unmatched_gdf_full.geometry.geom_type.isin(["LineString", "MultiLineString"])

                points_gdf = unmatched_gdf_full[point_mask].copy()
                polygons_gdf = unmatched_gdf_full[polygon_mask].copy()
                lines_gdf    = unmatched_gdf_full[line_mask].copy()

                self.log.emit(f"Points: {len(points_gdf)}, Polygons: {len(polygons_gdf)}, Lines: {len(lines_gdf)}")

                # ── 1A: Handle point‐based intersection with direct checks ──
                if not points_gdf.empty:
                    # Validate and repair geometries
                    points_gdf["geometry"] = points_gdf["geometry"].apply(validate_and_repair_geometry)
                    points_gdf = points_gdf[points_gdf["geometry"].notnull()]

                    # For each point, use spatial index candidates + simple geometry checks
                    for idx_row, row in points_gdf.iterrows():
                        if not self._is_running:
                            break

                        original_idx = row["index"]  # original index in self.gdf
                        point_geom = row["geometry"]

                        # First find candidate settlements using bounding‐box
                        candidate_idx = sorted(list(settlements_gdf.sindex.intersection(point_geom.bounds)))
                        if not candidate_idx:
                            continue  # no intersecting settlement → skip

                        matched_intersect_id = None

                        # Pass 1: strict membership (contains/touches)
                        for cand in candidate_idx:
                            settlement_geom = settlements_gdf.geometry.iloc[cand]
                            try:
                                if settlement_geom.contains(point_geom) or settlement_geom.touches(point_geom):
                                    matched_intersect_id = cand
                                    break
                            except Exception as e:
                                self.log.emit(f"Skipping invalid geometry intersection for point {original_idx}: {str(e)}")
                                continue

                        # Pass 2: fallback to plain intersects
                        if matched_intersect_id is None:
                            for cand in candidate_idx:
                                settlement_geom = settlements_gdf.geometry.iloc[cand]
                                try:
                                    if settlement_geom.intersects(point_geom):
                                        matched_intersect_id = cand
                                        break
                                except Exception as e:
                                    self.log.emit(f"Skipping invalid fallback intersection for point {original_idx}: {str(e)}")
                                    continue

                        if matched_intersect_id is not None:
                            settlement = settlements_gdf.iloc[matched_intersect_id]
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

                            if key and settlement.get("id") is not None:
                                data = {key: int(settlement["id"])}
                                if parent != "settlement" and settlement.get("settlement_id") is not None:
                                    data["settlement_id"] = int(settlement["settlement_id"])
                                if parent != "ward" and settlement.get("ward_id") is not None:
                                    data["ward_id"] = int(settlement["ward_id"])
                                if parent != "subcounty" and settlement.get("subcounty_id") is not None:
                                    data["subcounty_id"] = int(settlement["subcounty_id"])
                                if parent != "county" and settlement.get("county_id") is not None:
                                    data["county_id"] = int(settlement["county_id"])

                                with lock:
                                    pcode_entity_data[original_idx] = data
                                    valid_feature_indices.append(original_idx)
                                    processed_items += 1
                                    self.log.emit(
                                        f"(Point) Assigned {parent}-based data for index {original_idx}: {data}"
                                    )

                # ── 1B: Handle polygon‐based intersection by "maximum overlap area" ──
                if not polygons_gdf.empty:
                    # Validate and repair geometries before intersection
                    polygons_gdf["geometry"] = polygons_gdf["geometry"].apply(validate_and_repair_geometry)
                    polygons_gdf = polygons_gdf[polygons_gdf["geometry"].notnull()]
                    
                    if not polygons_gdf.empty:
                        # Build a spatial index on settlements for faster bounding‐box lookups
                        settlement_sindex = settlements_gdf.sindex

                    for idx_row, row in polygons_gdf.iterrows():
                        if not self._is_running:
                            break

                        original_idx = row["index"]
                        poly_geom = row["geometry"]

                        # 1B.1: Find candidate settlements by bounding‐box intersection
                        candidate_idx = list(settlement_sindex.intersection(poly_geom.bounds))
                        if not candidate_idx:
                            continue  # no intersecting settlement → skip

                        # 1B.2: Now compute exact intersection areas with each candidate
                        best_idx = None
                        best_area = 0.0
                        for cand in candidate_idx:
                            settlement_geom = settlements_gdf.geometry.iloc[cand]
                            try:
                                if not settlement_geom.intersects(poly_geom):
                                    continue
                                intersect_geom = settlement_geom.intersection(poly_geom)
                                if not intersect_geom.is_empty:
                                    area = intersect_geom.area
                                    if area > best_area:
                                        best_area = area
                                        best_idx = cand
                            except Exception as e:
                                self.log.emit(f"Skipping invalid geometry intersection for index {original_idx}: {str(e)}")
                                continue

                        # 1B.3: If best_idx is not None, assign the parent from that settlement
                        if best_idx is not None:
                            settlement = settlements_gdf.iloc[best_idx]
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

                            if key and settlement.get("id") is not None:
                                data = {key: int(settlement["id"])}
                                if parent != "settlement" and settlement.get("settlement_id") is not None:
                                    data["settlement_id"] = int(settlement["settlement_id"])
                                if parent != "ward" and settlement.get("ward_id") is not None:
                                    data["ward_id"] = int(settlement["ward_id"])
                                if parent != "subcounty" and settlement.get("subcounty_id") is not None:
                                    data["subcounty_id"] = int(settlement["subcounty_id"])
                                if parent != "county" and settlement.get("county_id") is not None:
                                    data["county_id"] = int(settlement["county_id"])

                                with lock:
                                    pcode_entity_data[original_idx] = data
                                    valid_feature_indices.append(original_idx)
                                    processed_items += 1
                                    self.log.emit(
                                        f"(Polygon) Assigned {parent}-based data for index {original_idx} "
                                        f"with overlap area {best_area:.6f}: {data}"
                                    )

                # ── 1C: Handle line‐based intersection via plain intersects ──
                if not lines_gdf.empty:
                    # Validate and repair geometries before intersection
                    lines_gdf["geometry"] = lines_gdf["geometry"].apply(validate_and_repair_geometry)
                    lines_gdf = lines_gdf[lines_gdf["geometry"].notnull()]
                    
                    if not lines_gdf.empty:
                        self.log.emit(f"Processing {len(lines_gdf)} line features...")
                        # Build a spatial index on settlements (just once)
                        settlement_sindex = settlements_gdf.sindex

                    for idx_row, row in lines_gdf.iterrows():
                        if not self._is_running:
                            break

                        original_idx = row["index"]       # keep track of the original index
                        line_geom    = row["geometry"]    # this is the LineString/MultiLineString

                        # Ensure geometry is valid
                        if line_geom is None or not line_geom.is_valid:
                            self.log.emit(f"Skipping invalid line geometry at index {original_idx}")
                            continue

                        # 1) Find candidate settlements by bounding‐box intersection
                        candidate_idx = list(settlement_sindex.intersection(line_geom.bounds))
                        if not candidate_idx:
                            self.log.emit(f"No candidate settlements found for line at index {original_idx}")
                            continue

                        # 2) Of those candidates, pick the first one that truly intersects
                        matched_settlement = None
                        for cand in candidate_idx:
                            settlement_geom = settlements_gdf.geometry.iloc[cand]
                            try:
                                if line_geom.intersects(settlement_geom):
                                    matched_settlement = settlements_gdf.iloc[cand]
                                    self.log.emit(f"Found intersection for line at index {original_idx} with settlement {cand}")
                                    break
                            except Exception as e:
                                self.log.emit(f"Skipping invalid geometry intersection for line {original_idx}: {str(e)}")
                                continue

                        if matched_settlement is not None:
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

                            if key and matched_settlement.get("id") is not None:
                                data = {key: int(matched_settlement["id"])}
                                # copy up the other parent‐IDs if needed:
                                if parent != "settlement" and matched_settlement.get("settlement_id") is not None:
                                    data["settlement_id"] = int(matched_settlement["settlement_id"])
                                if parent != "ward" and matched_settlement.get("ward_id") is not None:
                                    data["ward_id"] = int(matched_settlement["ward_id"])
                                if parent != "subcounty" and matched_settlement.get("subcounty_id") is not None:
                                    data["subcounty_id"] = int(matched_settlement["subcounty_id"])
                                if parent != "county" and matched_settlement.get("county_id") is not None:
                                    data["county_id"] = int(matched_settlement["county_id"])

                                with lock:
                                    pcode_entity_data[original_idx] = data
                                    valid_feature_indices.append(original_idx)
                                    processed_items += 1
                                    self.log.emit(
                                        f"(Line) Assigned {parent}-based data for index {original_idx}: {data}"
                                    )

            # ── STEP 2: Pcode matching for unmatched rows ─────────────────────────────────────
            unmatched_indices = [
                row_idx
                for row_idx, _ in self.gdf.iterrows()
                if row_idx not in pcode_entity_data
            ]
            
            if has_pcode and unmatched_indices:
                # Filter to only rows that actually have pcode values
                index_to_pcode = [
                    (row_idx, self.gdf.loc[row_idx, "pcode"])
                    for row_idx in unmatched_indices
                    if row_idx in self.gdf.index and self.gdf.loc[row_idx].get("pcode")
                ]
                self.log.emit(f"Found {len(index_to_pcode)} unmatched rows with pcode values for fallback matching.")

                if index_to_pcode:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = []
                        for start in range(0, len(index_to_pcode), batch_size):
                            batch_indices = index_to_pcode[start : start + batch_size]
                            batch_codes = [p for _, p in batch_indices]
                            batch_num = (start // batch_size) + 1
                            futures.append(
                                executor.submit(
                                    self.process_pcode_batch,
                                    start,
                                    batch_indices,
                                    batch_codes,
                                    batch_num,
                                    len(index_to_pcode),
                                    batch_size,
                                )
                            )

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
                                self.progress.emit(min(progress, 100))  # up to 100% for pcode

            # ── FINALIZE ─────────────────────────────────────────────────
            if pcode_entity_data:
                self.log.emit(
                    f"Data fetched successfully for {len(valid_feature_indices)} rows "
                    f"with parent entity '{self.parent_entity_name}'."
                )
            else:
                self.log.emit(f"No data fetched for parent entity '{self.parent_entity_name}'.")

            self.result.emit(pcode_entity_data, valid_feature_indices)
            self.finished.emit()

        except Exception as e:
            self.log.emit(f"Error fetching entity data: {str(e)}")
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


            self.finished.emit()


def _parse_kesmis_error_response(response):
    """Extract a user-facing message from a KeSMIS API error response."""
    try:
        body = response.json() if response.content else {}
    except (ValueError, json.JSONDecodeError):
        body = {}

    if isinstance(body, dict):
        message = body.get("message") or body.get("error")
        if body.get("code") == "DEVICE_LIMIT_REACHED":
            lines = [
                message
                or "Too many active sessions. Sign out on another browser or device, or ask an administrator to clear your sessions."
            ]
            devices = body.get("activeDevices") or []
            if devices:
                lines.append("")
                lines.append("Active sessions:")
                for device in devices:
                    label = device.get("deviceLabel") or "Unknown device"
                    ip = device.get("ipAddress")
                    suffix = f" ({ip})" if ip else ""
                    lines.append(f"• {label}{suffix}")
            return "\n".join(lines)
        if message:
            return str(message)

    text = (response.text or "").strip()
    if text and not text.startswith("<"):
        return text[:500]
    return f"Login failed (HTTP {response.status_code})."


def _format_import_http_error(response, fallback, format_error_lines=None):
    """Extract KeSMIS import/upsert error details from an HTTP response."""
    try:
        body = response.json() if response.content else {}
    except (ValueError, json.JSONDecodeError):
        body = {}

    if isinstance(body, dict):
        parts = []
        for key in ("message", "error", "detail"):
            value = body.get(key)
            if value:
                parts.append(str(value))
        errors = body.get("errors")
        if errors and format_error_lines:
            parts.extend(format_error_lines(errors, limit=5))
        elif errors:
            parts.append(str(errors)[:800])
        if parts:
            return f"HTTP {response.status_code}: " + "\n".join(parts)

    text = (response.text or "").strip()
    if text and not text.startswith("<"):
        return f"HTTP {response.status_code}: {text[:800]}"
    return fallback


def kesmis_validate_token(url, token):
    """Return True when a saved KeSMIS token is still valid."""
    if not url or not token:
        return False
    headers = {"Authorization": f"Bearer {token}", "x-access-token": token}
    try:
        response = requests.get(f"{url.rstrip('/')}/api/v1/models/list", headers=headers, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


def kesmis_sign_in(url, username, password):
    """Sign in to KeSMIS. Returns (token, error_message, is_device_limit)."""
    url = url.rstrip("/")
    if not url or not username or not password:
        return None, "URL, username, and password are required.", False
    if not url.startswith(("http://", "https://")):
        return None, "Server URL must start with http:// or https://", False

    login_url = f"{url}/api/auth/signin"
    try:
        response = requests.post(
            login_url,
            json={"username": username, "password": password},
            timeout=10,
        )
    except requests.Timeout:
        return None, "The server did not respond. Please check the URL and try again.", False
    except requests.ConnectionError:
        return None, "Could not connect to the server. Please check your network and URL.", False

    if response.status_code == 200:
        try:
            body = response.json()
        except (ValueError, json.JSONDecodeError):
            return None, "Login failed: invalid response from server.", False
        token = body.get("data") or body.get("accessToken")
        if not token:
            return None, "Login failed: no token received from server.", False
        return token, None, False

    try:
        body = response.json() if response.content else {}
    except (ValueError, json.JSONDecodeError):
        body = {}
    is_device_limit = isinstance(body, dict) and body.get("code") == "DEVICE_LIMIT_REACHED"
    return None, _parse_kesmis_error_response(response), is_device_limit


class KesMISLoginDialog(QDialog):
    """Collect KeSMIS URL and credentials before opening the import dialog."""

    def __init__(self, parent=None):
        super().__init__(parent)
        configure_qgis_dialog(self, parent)
        self.setWindowTitle("KeSMIS Login")

        self.settings = QSettings("YourOrganization", "KesMIS")
        self.url = ""
        self.username = ""
        self.password = ""
        self.token = None
        self._login_in_progress = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(4)

        self._form_widget = QWidget()
        form_layout = QVBoxLayout(self._form_widget)
        form_layout.setContentsMargins(0, 0, 0, 0)
        form_layout.setSpacing(4)
        intro = QLabel("Enter the KeSMIS server URL, username, and password to continue.")
        intro.setWordWrap(True)
        intro.setContentsMargins(0, 0, 0, 0)
        form_layout.addWidget(intro)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setVerticalSpacing(4)
        form.setHorizontalSpacing(8)
        self.url_input = QLineEdit(self.settings.value("url", "http://localhost"))
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("Username")
        self.username_input.setText(self.settings.value("username", ""))
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("Password")
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setText(self.settings.value("password", ""))
        form.addRow("Server URL:", self.url_input)
        form.addRow("Username:", self.username_input)
        form.addRow("Password:", self.password_input)
        form_layout.addLayout(form)

        self.save_credentials = QCheckBox("Save credentials")
        self.save_credentials.setChecked(bool(self.settings.value("username") and self.settings.value("password")))
        form_layout.addWidget(self.save_credentials)

        buttons = QHBoxLayout()
        buttons.addStretch()
        self.login_button = QPushButton("Login")
        self.login_button.setDefault(True)
        self.login_button.clicked.connect(self.login)
        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        buttons.addWidget(self.login_button)
        buttons.addWidget(cancel_button)
        form_layout.addLayout(buttons)

        self._busy_widget = QWidget()
        busy_layout = QVBoxLayout(self._busy_widget)
        busy_layout.setContentsMargins(24, 24, 24, 24)
        self._busy_label = QLabel("Logging in…")
        self._busy_label.setAlignment(Qt.AlignCenter)
        busy_layout.addWidget(self._busy_label)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        busy_layout.addWidget(self.progress_bar)

        self._stack = QStackedWidget()
        self._stack.addWidget(self._form_widget)
        self._stack.addWidget(self._busy_widget)
        layout.addWidget(self._stack)

        if self.settings.value("auth_token", "") and self.url_input.text().strip():
            self._show_busy()
            QTimer.singleShot(0, self._try_auto_login)
        else:
            self._show_form()

    def _show_busy(self, message="Logging in…"):
        self._busy_label.setText(message)
        self._stack.setCurrentWidget(self._busy_widget)
        self.setFixedSize(340, 130)
        QApplication.processEvents()

    def _show_form(self):
        self._stack.setCurrentWidget(self._form_widget)
        self.setFixedSize(460, 230)

    def _try_auto_login(self):
        saved_token = self.settings.value("auth_token", "")
        url = self.url_input.text().strip()
        if not saved_token or not url:
            self._show_form()
            return

        self._show_busy("Logging in…")
        if kesmis_validate_token(url, saved_token):
            self.url = url.rstrip("/")
            self.username = self.username_input.text()
            self.password = self.password_input.text()
            self.token = saved_token
            self.accept()
            return

        self.settings.remove("auth_token")
        self._show_form()

    def login(self):
        if self._login_in_progress:
            return

        url = self.url_input.text().strip()
        username = self.username_input.text()
        password = self.password_input.text()
        if not url or not username or not password:
            QMessageBox.critical(self, "Input Error", "Please provide server URL, username, and password.")
            return

        self._login_in_progress = True
        self.login_button.setEnabled(False)
        self._show_busy("Logging in…")

        try:
            token, error, is_device_limit = kesmis_sign_in(url, username, password)
            if error:
                self._show_form()
                if is_device_limit:
                    QMessageBox.warning(self, "Too many active sessions", error)
                else:
                    QMessageBox.critical(self, "Login Error", error)
                return

            self.url = url.rstrip("/")
            self.username = username
            self.password = password
            self.token = token

            if self.save_credentials.isChecked():
                self.settings.setValue("url", self.url)
                self.settings.setValue("username", username)
                self.settings.setValue("password", password)
                self.settings.setValue("auth_token", token)
            else:
                self.settings.remove("auth_token")

            self.accept()
        finally:
            self._login_in_progress = False
            self.login_button.setEnabled(True)


class KesMISDialog(QDialog, CollapsibleHelpMixin):
    def __init__(self, parent=None, server_url="", username="", token=None):
        super().__init__(parent)
        configure_qgis_dialog(self, parent)
        self.setWindowTitle("Export data to KeSMIS")
        resize_dialog_to_screen(self, min_width=720, min_height=600, max_width=960, max_height=800)

        # Initialize variables
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.token = token
        self.api_entities = []
        self.field_mapping = {}
        self.pcode_fields = ["settlement_id", "ward_id", "subcounty_id", "county_id"]
        self.is_logged_in = bool(self.token)
        self.settings = QSettings("YourOrganization", "KesMIS")
        self.valid_feature_indices = []
        self.gdf = None
        self._full_table_data = []

        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)

        connection_box = QGroupBox("Server")
        connection_layout = QHBoxLayout()
        self.connection_label = QLabel()
        self.connection_label.setWordWrap(True)
        connection_layout.addWidget(self.connection_label, 1)
        self.logout_button = QPushButton("Logout")
        self.logout_button.setToolTip("Sign out and close this dialog")
        self.logout_button.clicked.connect(self._logout)
        connection_layout.addWidget(self.logout_button)
        connection_box.setLayout(connection_layout)
        main_layout.addWidget(connection_box)

        # Layer and Parent Selection
        layer_box = QGroupBox("Layer and Parent Selection")
        layer_layout = QVBoxLayout()

        settlement_layout = QHBoxLayout()
        self.settlement_layer_combo = QComboBox()
        self.settlement_layer_combo.setEnabled(False)
        self.settlement_layer_combo.currentTextChanged.connect(self._update_code_guidance)
        self.sync_settlement_codes_button = QPushButton("Sync Settlements with KeSMIS")
        self.sync_settlement_codes_button.clicked.connect(self.sync_settlement_codes_for_selection)
        self.sync_settlement_codes_button.setEnabled(False)
        self.sync_settlement_codes_button.setToolTip(
            "Match the settlement layer to KeSMIS by geometry, map fields, then create or update records on the server."
        )
        settlement_layout.addWidget(QLabel("Select Settlement Layer:"))
        settlement_layout.addWidget(self.settlement_layer_combo, 1)
        settlement_layout.addWidget(self.sync_settlement_codes_button)

        self.code_guidance_label = QLabel("")
        self.code_guidance_label.setWordWrap(True)
        self.code_guidance_label.setStyleSheet("color: #8a4b00;")

        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        self._progress_anim_timer = QTimer(self)
        self._progress_anim_timer.setInterval(75)
        self._progress_anim_timer.timeout.connect(self._tick_progress_animation)
        self._progress_anim_mode = None
        self._progress_anim_message = ""
        self._progress_anim_step = 0
        self._progress_anim_value = 0
        self._progress_anim_direction = 1

        layer_selection_layout = QHBoxLayout()
        self.layer_combo = QComboBox()
        self.layer_combo.setEnabled(False)
        self.layer_combo.currentTextChanged.connect(self.reset_data)
        self.layer_combo.currentTextChanged.connect(self._update_code_guidance)
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
        self.entity_combo.activated.connect(self._on_entity_activated)
        self.entity_combo.setPlaceholderText("Search entities...")
        entity_selection_layout.addWidget(QLabel("Select Entity:"))
        entity_selection_layout.addWidget(self.entity_combo)
        layer_layout.addLayout(settlement_layout)
        layer_layout.addWidget(self.code_guidance_label)
        layer_layout.addWidget(self.progress_bar)
        layer_layout.addLayout(layer_selection_layout)
        layer_layout.addLayout(parent_selection_layout)
        layer_layout.addLayout(entity_selection_layout)
        layer_box.setLayout(layer_layout)
        layer_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        main_layout.addWidget(layer_box)
        mapping_box = QGroupBox("Field Mapping")
        mapping_layout = QVBoxLayout()
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
        
        # Dry Run Options
        dry_run_layout = QHBoxLayout()
        self.dry_run_checkbox = QCheckBox("Dry Run (Test Mode)")
        self.dry_run_checkbox.setChecked(False)
        self.dry_run_checkbox.setToolTip(
            "Validate a limited number of records on the server without saving them"
        )
        dry_run_layout.addWidget(self.dry_run_checkbox)
        dry_run_layout.addWidget(QLabel("Number of records:"))
        self.dry_run_spinbox = QSpinBox()
        self.dry_run_spinbox.setMinimum(1)
        self.dry_run_spinbox.setMaximum(1000)
        self.dry_run_spinbox.setValue(10)
        self.dry_run_spinbox.setEnabled(False)
        self.dry_run_spinbox.setToolTip("Number of records to validate in dry run mode")
        self.dry_run_checkbox.toggled.connect(self.dry_run_spinbox.setEnabled)
        self.dry_run_checkbox.toggled.connect(self.update_submit_button_text)
        self.dry_run_spinbox.valueChanged.connect(self.update_submit_button_text)
        dry_run_layout.addWidget(self.dry_run_spinbox)
        dry_run_layout.addStretch()
        mapping_layout.addLayout(dry_run_layout)
        
        self.submit_button = QPushButton("Submit Data to KeSMIS")
        self.submit_button.setEnabled(False)
        self.submit_button.clicked.connect(self.submit_features)
        mapping_layout.addWidget(self.submit_button)
        mapping_box.setLayout(mapping_layout)

        # Log Display
        log_box = QGroupBox("Log")
        log_layout = QVBoxLayout()
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        self.log_textedit.setFixedHeight(100)
        self.log_textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        log_layout.addWidget(self.log_textedit)
        log_actions = QHBoxLayout()
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)
        log_actions.addWidget(self.clear_log_button)
        log_actions.addStretch()
        log_layout.addLayout(log_actions)
        log_box.setLayout(log_layout)

        main_layout.addWidget(mapping_box)
        main_layout.addWidget(log_box)

        # Create scroll area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(main_widget)

        work_panel = QWidget()
        work_layout = QVBoxLayout(work_panel)
        work_layout.setContentsMargins(0, 0, 0, 0)
        work_layout.addWidget(scroll_area)

        self._attach_collapsible_help(work_panel, self._help_html(), add_toggle_row=False)
        log_actions.addWidget(self.toggle_help_button)

        # Thread for background processing
        self.thread = QThread()
        self.worker = None
        self.field_matching_worker = None
        self._setup_after_login()

    def _setup_after_login(self):
        """Populate the import dialog after a successful KeSMIS login."""
        self.connection_label.setText(f"Connected to {self.server_url} as {self.username}")
        self.log_message(f"Logged in to {self.server_url} as {self.username}")
        self.populate_settlement_layers()
        self.populate_layers()
        self.fetch_entities(self.server_url)
        self.layer_combo.setEnabled(True)
        self.parent_combo.setEnabled(True)
        self.entity_combo.setEnabled(True)
        self._update_code_guidance()

    def _logout(self):
        """Sign out of KeSMIS and close the import dialog."""
        self.token = None
        self.is_logged_in = False
        self.settings.remove("auth_token")
        self.log_message(f"Logged out from {self.server_url}")
        self.reject()

    @staticmethod
    def _help_html():
        return """
        <h3>Import to KeSMIS</h3>
        <p>Upload QGIS vector layer features to a KeSMIS server with automatic field mapping and parent-entity assignment.</p>

        <h4>Quick start</h4>
        <ol>
            <li><b>Server login</b> &mdash; enter the KeSMIS URL, username, and password when prompted.</li>
            <li><b>Settlement sync</b> &mdash; choose the settlement layer, click <b>Sync Settlements with KeSMIS</b>, review field mapping and geometry matches, then create or update records on the server.</li>
            <li><b>Select Layer</b> &mdash; choose the QGIS vector layer to export.</li>
            <li><b>Parent entity</b> &mdash; pick <code>settlement</code> or <code>ward</code> for spatial matching.</li>
            <li><b>Select Entity</b> &mdash; choose the API entity/model to submit to.</li>
            <li>Review <b>Field Mapping</b>, then click <b>Submit Data to KeSMIS</b>.</li>
        </ol>

        <h4>Field mapping</h4>
        <p>Layer fields are matched to API fields automatically. Use the filter box to search mappings and adjust API field selections in the table.</p>

        <h4>Dry run</h4>
        <p>Enable <b>Dry Run</b> to validate a limited number of records on the server. Nothing is saved until you run a full submission.</p>

        <h4>Code column</h4>
        <p>When you select a layer, the plugin checks for a <code>code</code> field. If it is missing on a non-settlement layer, you can allow automatic code generation or cancel.</p>
        <p><b>Settlement layers:</b> do not generate random codes. Use <b>Sync Settlements with KeSMIS</b> to map fields, match by geometry, and create or update settlement records on the server.</p>
        """

    SETTLEMENT_SYNC_FIELD = "kesmis_sync"
    SETTLEMENT_SKIP_LAYER_FIELDS = {
        "fid", "objectid", "globalid", "ogc_fid",
        "shape_leng", "shape_length", "shape_area", "perimeter", "area",
    }
    SETTLEMENT_SKIP_API_FIELDS = {
        "id", "geom", "geojson", "geometry", "createdat", "updatedat",
        "created_at", "updated_at", "isapproved",
    }

    def _start_progress_animation(self, message="", mode="marquee"):
        self._progress_anim_mode = mode
        self._progress_anim_message = message
        self._progress_anim_step = 0
        self._progress_anim_value = 5
        self._progress_anim_direction = 1
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setRange(0, 100)
        if mode == "marquee":
            self.progress_bar.setValue(5)
        if not self._progress_anim_timer.isActive():
            self._progress_anim_timer.start()

    def _stop_progress_animation(self):
        if self._progress_anim_timer.isActive():
            self._progress_anim_timer.stop()
        self._progress_anim_mode = None

    def _tick_progress_animation(self):
        self._progress_anim_step += 1
        dots = "." * ((self._progress_anim_step % 3) + 1)
        message = self._progress_anim_message or "Working"
        if self._progress_anim_mode == "marquee":
            self._progress_anim_value += 5 * self._progress_anim_direction
            if self._progress_anim_value >= 90:
                self._progress_anim_value = 90
                self._progress_anim_direction = -1
            elif self._progress_anim_value <= 5:
                self._progress_anim_value = 5
                self._progress_anim_direction = 1
            self.progress_bar.setValue(self._progress_anim_value)
            self.progress_bar.setFormat(f"{message}{dots}")
        elif self._progress_anim_mode == "determinate":
            self.progress_bar.setFormat(f"{message}{dots}  %p%")

    def _start_settlement_sync_progress(self, message="Starting settlement sync..."):
        self.progress_bar.setVisible(True)
        self._start_progress_animation(message, mode="marquee")

    def _update_settlement_sync_progress(self, value=None, message=None, indeterminate=False):
        if message:
            self._progress_anim_message = message
        if indeterminate:
            self._start_progress_animation(
                message or self._progress_anim_message,
                mode="marquee",
            )
            return

        self._progress_anim_mode = "determinate"
        self.progress_bar.setRange(0, 100)
        if value is not None:
            self.progress_bar.setValue(min(max(int(value), 0), 100))
        if not self._progress_anim_timer.isActive():
            self._progress_anim_timer.start()
        self._tick_progress_animation()
        QApplication.processEvents()

    def _finish_settlement_sync_progress(self):
        self._stop_progress_animation()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("")
        self.progress_bar.setVisible(False)
        QApplication.processEvents()

    def _get_settlement_layers(self):
        return [
            layer for layer in QgsProject.instance().mapLayers().values()
            if isinstance(layer, QgsVectorLayer) and is_settlement_data_layer(layer.name())
        ]

    def _is_layer_kesmis_synced(self, layer):
        """A layer is marked synced when the kesmis_sync field exists with any value."""
        field_names = {f.name().lower() for f in layer.fields()}
        if self.SETTLEMENT_SYNC_FIELD not in field_names:
            return False
        idx = -1
        for i, f in enumerate(layer.fields()):
            if f.name().lower() == self.SETTLEMENT_SYNC_FIELD:
                idx = i
                break
        if idx < 0:
            return False
        for feature in layer.getFeatures():
            value = feature[idx]
            if value is not None and str(value).strip():
                return True
        return False

    def _is_skipped_settlement_layer_field(self, field_name):
        lower = field_name.lower()
        if lower == self.SETTLEMENT_SYNC_FIELD.lower() or lower == "code":
            return True
        if lower in self.SETTLEMENT_SKIP_LAYER_FIELDS:
            return True
        return lower.startswith("shape_")

    def _get_writable_settlement_api_fields(self, entity):
        allowed = {"code", "geom", "isApproved"}
        allowed.update(self.pcode_fields)
        for attr in entity.get("attributes", []):
            name = attr.get("name")
            if not name:
                continue
            if attr.get("readOnly") or attr.get("readonly"):
                continue
            if name.lower() in self.SETTLEMENT_SKIP_API_FIELDS:
                continue
            allowed.add(name)
        return allowed

    def _entity_attr_lookup(self, entity):
        return {
            attr.get("name"): attr
            for attr in entity.get("attributes", [])
            if attr.get("name")
        }

    def _coerce_settlement_api_value(self, api_field, value, entity_attrs):
        if not self._has_meaningful_value(value):
            return None
        value = self._convert_to_serializable(value)
        if value is None:
            return None
        attr = entity_attrs.get(api_field) or {}
        attr_type = str(attr.get("type") or "").lower()
        if attr_type in ("boolean", "bool"):
            if isinstance(value, str):
                return value.strip().lower() in ("true", "1", "yes", "y")
            return bool(value)
        if attr_type in ("integer", "int", "number") and "float" not in attr_type:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
        if attr_type in ("float", "double", "decimal", "number"):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        return value

    @staticmethod
    def _has_meaningful_value(value):
        if value is None:
            return False
        if isinstance(value, str) and not value.strip():
            return False
        return True

    def _build_settlement_geometry(self, geom):
        if geom is None or geom.is_empty:
            return None
        geom = validate_and_repair_geometry(self.to_2d(geom))
        if geom is None or geom.is_empty:
            return None
        return self._convert_to_serializable(geom.__geo_interface__)

    def _fetch_kesmis_geo_gdf(self, model, url, headers):
        for label, endpoint in (
            ("geo/minimal", f"{url}/api/v1/data/geo/minimal"),
            ("geo/full", f"{url}/api/v1/data/geo"),
        ):
            try:
                response = requests.get(
                    endpoint,
                    headers=headers,
                    params={"model": model},
                    timeout=60,
                )
                response.raise_for_status()
                gdf = self._build_gdf_from_geojson(response.json())
                if not gdf.empty:
                    self.log_message(f"Loaded {len(gdf)} KeSMIS {model} feature(s) from {label}.")
                    return gdf
            except Exception as e:
                self.log_message(f"KeSMIS {model} fetch via {label} failed: {e}")
        return None

    def _lookup_ward_parent_ids(self, geom, wards_gdf):
        """Return ward/subcounty/county IDs for a geometry using ward boundaries."""
        if geom is None or wards_gdf is None or wards_gdf.empty:
            return {}

        if geom.geom_type in ("Polygon", "MultiPolygon"):
            test_geom = geom.centroid
        else:
            test_geom = geom
        if test_geom is None or test_geom.is_empty:
            return {}

        candidate_idx = list(wards_gdf.sindex.intersection(test_geom.bounds))
        for cand in candidate_idx:
            ward_geom = wards_gdf.geometry.iloc[cand]
            try:
                if not (ward_geom.contains(test_geom) or ward_geom.intersects(test_geom)):
                    continue
            except Exception:
                continue
            ward = wards_gdf.iloc[cand]
            parent_ids = {}
            if ward.get("id") is not None:
                parent_ids["ward_id"] = int(ward["id"])
            for key in ("subcounty_id", "county_id"):
                if ward.get(key) is not None:
                    try:
                        parent_ids[key] = int(ward[key])
                    except (TypeError, ValueError):
                        pass
            if parent_ids:
                return parent_ids
        return {}

    def _get_settlement_entity(self):
        for entity in self.api_entities:
            if entity.get("model", "").lower() == "settlement":
                return entity
        return None

    def _auto_match_fields(self, layer, entity, exclude_fields=None):
        """Match layer fields to settlement API attributes (same logic as FieldMatchingWorker)."""
        exclude_fields = exclude_fields or set()
        exclude_fields.add(self.SETTLEMENT_SYNC_FIELD)
        layer_fields = [
            f.name() for f in layer.fields()
            if f.name not in exclude_fields and not self._is_skipped_settlement_layer_field(f.name())
        ]
        writable_api_fields = sorted(
            self._get_writable_settlement_api_fields(entity) - {"code", "geom", "isApproved"},
            key=lambda x: x.lower(),
        )
        field_mapping = {}
        scores = {}
        table_data = []

        for field in layer_fields:
            candidates = process.extract(
                field,
                writable_api_fields,
                scorer=fuzz.ratio,
                score_cutoff=70,
            )
            if candidates:
                best_field, best_score, _ = max(candidates, key=lambda x: x[1])
                field_mapping[field] = best_field
                scores[field] = best_score
                table_data.append((field, best_field, str(int(best_score))))
            else:
                field_mapping[field] = None
                table_data.append((field, "", "-"))

        reverse_map = {}
        for layer_field, api_field in field_mapping.items():
            if api_field:
                reverse_map.setdefault(api_field, []).append(layer_field)

        for api_field, layer_list in reverse_map.items():
            if len(layer_list) > 1:
                sorted_layers = sorted(layer_list, key=lambda lf: scores.get(lf, 0), reverse=True)
                for duplicate in sorted_layers[1:]:
                    field_mapping[duplicate] = None
                    for idx, row in enumerate(table_data):
                        if row[0] == duplicate:
                            table_data[idx] = (duplicate, "", "-")
                            break

        return field_mapping, table_data

    def _update_code_guidance(self):
        settlement_layers = self._get_settlement_layers()
        selected_settlement = self.settlement_layer_combo.currentData()

        if selected_settlement:
            if self._is_layer_kesmis_synced(selected_settlement):
                self.code_guidance_label.setText(
                    _join_ui_text(
                        "Settlement layer '",
                        selected_settlement.name(),
                        "' was previously synced with KeSMIS (marked in '",
                        self.SETTLEMENT_SYNC_FIELD,
                        "'). You can run sync again to push field updates or create new settlements.",
                    )
                )
            else:
                self.code_guidance_label.setText(
                    _join_ui_text(
                        "Settlement layer selected: '",
                        selected_settlement.name(),
                        "'. Click Sync Settlements with KeSMIS to match by geometry, map fields, "
                        "and create or update records on the server.",
                    )
                )
            self.sync_settlement_codes_button.setEnabled(bool(self.token))
        elif settlement_layers:
            layer_names = ", ".join(layer.name() for layer in settlement_layers)
            self.code_guidance_label.setText(
                _join_ui_text(
                    "Settlement layer(s) detected: ",
                    layer_names,
                    ". Select a settlement layer from the dropdown above, then click "
                    "Sync Settlements with KeSMIS.",
                )
            )
            self.sync_settlement_codes_button.setEnabled(False)
        else:
            self.code_guidance_label.setText(
                "Select a layer to import. A code field is required; you will be prompted "
                "to generate codes if the selected layer does not have one."
            )
            self.sync_settlement_codes_button.setEnabled(False)

    def _layer_has_code_field(self, layer):
        return any(field.name().lower() == "code" for field in layer.fields())

    def _clear_layer_selection(self):
        self.layer_combo.blockSignals(True)
        if self.layer_combo.count() > 0:
            self.layer_combo.setCurrentIndex(0)
        self.layer_combo.blockSignals(False)

    def _ensure_layer_code_ready(self, layer):
        """Ensure the selected layer can proceed; prompt to generate codes when needed."""
        if is_settlement_data_layer(layer.name()):
            QMessageBox.warning(
                self,
                "Settlement Layer",
                f"'{layer.name()}' looks like a settlement layer.\n\n"
                "Use Sync Settlements with KeSMIS for settlement layers instead of "
                "generating random codes here.",
            )
            self._clear_layer_selection()
            return False

        if self._layer_has_code_field(layer):
            return True

        reply = QMessageBox.question(
            self,
            "Missing Code Field",
            f"The layer '{layer.name()}' has no 'code' field.\n\n"
            "Generate unique codes for features that need them?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes,
        )
        if reply != QMessageBox.Yes:
            QMessageBox.information(
                self,
                "Code Required",
                "Import cannot continue without a 'code' field on the selected layer.",
            )
            self._clear_layer_selection()
            return False

        if not process_layer(layer, log=self.log_message):
            QMessageBox.warning(
                self,
                "Code Generation Failed",
                f"Could not add or fill the 'code' field on '{layer.name()}'.\n\n"
                "See the log for details.",
            )
            self._clear_layer_selection()
            return False

        self.log_message(f"Generated 'code' values for layer '{layer.name()}'.")
        return True

    def _feature_label(self, props, feature_id):
        for key in ("name", "settlement_name", "Name", "label", "settlement", "title"):
            value = props.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return f"Feature {feature_id}"

    def _kesmis_label(self, kesmis_row):
        for key in ("name", "settlement_name", "Name", "label", "title"):
            if key in kesmis_row and kesmis_row[key] is not None and str(kesmis_row[key]).strip():
                return str(kesmis_row[key]).strip()
        if kesmis_row.get("id") is not None:
            return f"Settlement ID {kesmis_row['id']}"
        code = kesmis_row.get("code")
        if code is not None and str(code).strip():
            return f"Code {code}"
        return "KeSMIS settlement"

    def _find_intersecting_settlements(self, feature_geom, settlements_gdf):
        """Return all KeSMIS settlements intersecting a feature, best overlap first."""
        if feature_geom is None or feature_geom.is_empty:
            return []

        candidate_idx = list(settlements_gdf.sindex.intersection(feature_geom.bounds))
        if not candidate_idx:
            return []

        results = []
        for cand in candidate_idx:
            settlement_geom = settlements_gdf.geometry.iloc[cand]
            if feature_geom.geom_type == "Point":
                if not (
                    settlement_geom.contains(feature_geom)
                    or settlement_geom.touches(feature_geom)
                    or settlement_geom.intersects(feature_geom)
                ):
                    continue
                overlap = 1.0
            else:
                if not settlement_geom.intersects(feature_geom):
                    continue
                try:
                    overlap = settlement_geom.intersection(feature_geom).area
                except Exception:
                    continue
                if overlap <= 0:
                    continue

            row = settlements_gdf.iloc[cand]
            kesmis_code = self._row_code(row)
            if not kesmis_code:
                continue
            kesmis_row = {
                k: self._convert_to_serializable(v)
                for k, v in row.items()
                if k != "geometry"
            }
            results.append(
                {
                    "kesmis_label": self._kesmis_label(row),
                    "kesmis_code": kesmis_code,
                    "kesmis_id": row.get("id"),
                    "kesmis_row": kesmis_row,
                    "overlap": overlap,
                }
            )

        results.sort(key=lambda item: item["overlap"], reverse=True)
        return results

    def _generate_settlement_short_code(self, existing_codes):
        while True:
            code = shortuuid.uuid()[:8]
            if code not in existing_codes:
                existing_codes.add(code)
                return code

    def _settlement_sync_status(self, current_code, kesmis_code, is_create=False):
        if is_create:
            return "Will create on KeSMIS"
        if not kesmis_code:
            return "No KeSMIS code available"
        if not current_code:
            return "Will update on KeSMIS"
        if current_code == kesmis_code:
            return "Will update on KeSMIS (code matches)"
        return "Will update on KeSMIS (replace local code)"

    def _settlement_transfer_status(self, current_code, kesmis_code, is_generated=False):
        if is_generated:
            return "Will generate new code"
        if not kesmis_code:
            return "No KeSMIS code available"
        if not current_code:
            return "Will copy code"
        if current_code == kesmis_code:
            return "Already matches"
        return "Will replace code"

    def _compute_settlement_matches(self, layer, settlements_gdf, layer_gdf=None, progress_callback=None):
        if layer_gdf is None:
            if progress_callback:
                progress_callback(28, "Loading layer features...", indeterminate=True)
            layer_gdf = self._build_gdf_from_layer(layer, include_fid=True)
        if layer_gdf.empty:
            return [], layer_gdf

        code_idx = None
        field_names = [f.name() for f in layer.fields()]
        if "code" in field_names:
            code_idx = layer.fields().indexOf("code")
        elif "code" in {name.lower(): idx for idx, name in enumerate(field_names)}:
            code_idx = {name.lower(): idx for idx, name in enumerate(field_names)}["code"]

        matches = []
        total = len(layer_gdf)
        for idx, (_, row) in enumerate(layer_gdf.iterrows()):
            feature_id = row.get("_qgis_fid")
            if feature_id is None:
                continue

            props = {k: v for k, v in row.items() if k not in ("geometry", "_qgis_fid")}
            local_label = self._feature_label(props, feature_id)
            current_code = ""
            if code_idx is not None and code_idx >= 0:
                value = layer.getFeature(feature_id)[code_idx]
                if value is not None:
                    current_code = str(value).strip()

            candidates = self._find_intersecting_settlements(row.geometry, settlements_gdf)
            matches.append(
                {
                    "feature_id": feature_id,
                    "local_label": local_label,
                    "current_code": current_code,
                    "candidates": candidates,
                }
            )
            if progress_callback and total:
                pct = 30 + int((idx + 1) / total * 35)
                progress_callback(
                    pct,
                    f"Matching settlements ({idx + 1}/{total})...",
                )

        return matches, layer_gdf

    def _resolve_settlement_matches(self, layer_name, matches, settlement_entity, field_mapping, mapping_table_data):
        """Let the user review field mapping and geometry matches before syncing to KeSMIS."""
        if not matches:
            return None, field_mapping

        dialog = QDialog(self)
        dialog.setWindowTitle("Review Settlement Sync")
        dialog.setMinimumSize(900, 560)

        layout = QVBoxLayout(dialog)
        multi_count = sum(1 for m in matches if len(m["candidates"]) > 1)
        no_match_count = sum(1 for m in matches if not m["candidates"])
        update_count = len(matches) - no_match_count
        intro = QLabel(
            f"Review sync for '{layer_name}'.\n"
            f"Matched to existing KeSMIS settlements (will update): {update_count}\n"
            f"Multiple KeSMIS intersections (choose match): {multi_count}\n"
            f"No intersection (will create new): {no_match_count}\n"
            "Review code matching first, then adjust field mapping on the second tab."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        api_fields = sorted(
            self._get_writable_settlement_api_fields(settlement_entity) - {"code", "geom", "isApproved"},
            key=lambda x: x.lower(),
        )

        tabs = QTabWidget()

        # Tab 1: code / geometry matching
        matching_tab = QWidget()
        matching_layout = QVBoxLayout(matching_tab)
        matching_layout.setContentsMargins(8, 8, 8, 8)

        table = QTableWidget(len(matches), 6)
        table.setHorizontalHeaderLabels(
            ["Local Settlement", "Current Code", "KeSMIS Match", "Code To Apply", "Action", "Status"]
        )
        table.horizontalHeader().setStretchLastSection(True)
        table.setEditTriggers(QTableWidget.NoEditTriggers)

        existing_codes = set()
        for match in matches:
            if match["current_code"]:
                existing_codes.add(match["current_code"])
            for candidate in match["candidates"]:
                existing_codes.add(candidate["kesmis_code"])

        def sync_status_for_selection(row_match, selected):
            is_create = selected.get("generated", False)
            return self._settlement_sync_status(
                row_match["current_code"],
                selected.get("kesmis_code"),
                is_create=is_create,
            )

        selectors = []
        for row, match in enumerate(matches):
            table.setItem(row, 0, QTableWidgetItem(match["local_label"]))
            table.setItem(row, 1, QTableWidgetItem(match["current_code"]))

            candidates = match["candidates"]
            if len(candidates) > 1:
                combo = QComboBox()
                for candidate in candidates:
                    combo.addItem(
                        f"{candidate['kesmis_label']} ({candidate['kesmis_code']})",
                        candidate,
                    )

                def update_combo_row(table_row, combo_box, row_match):
                    selected = combo_box.currentData()
                    is_create = selected.get("generated", False)
                    table.setItem(table_row, 3, QTableWidgetItem(selected["kesmis_code"]))
                    table.setItem(
                        table_row,
                        4,
                        QTableWidgetItem("Create on KeSMIS" if is_create else "Update on KeSMIS"),
                    )
                    table.setItem(
                        table_row,
                        5,
                        QTableWidgetItem(sync_status_for_selection(row_match, selected)),
                    )

                combo.currentIndexChanged.connect(
                    lambda _idx, table_row=row, combo_box=combo, row_match=match: update_combo_row(
                        table_row, combo_box, row_match
                    )
                )
                table.setCellWidget(row, 2, combo)
                selectors.append(("combo", combo))
                selected = candidates[0]
            elif len(candidates) == 1:
                selected = candidates[0]
                table.setItem(row, 2, QTableWidgetItem(selected["kesmis_label"]))
                selectors.append(("fixed", selected))
            else:
                generated = self._generate_settlement_short_code(existing_codes)
                selected = {
                    "kesmis_label": "No KeSMIS intersection — new settlement",
                    "kesmis_code": generated,
                    "generated": True,
                }
                table.setItem(row, 2, QTableWidgetItem(selected["kesmis_label"]))
                selectors.append(("generated", selected))

            is_create = selected.get("generated", False)
            table.setItem(row, 3, QTableWidgetItem(selected["kesmis_code"]))
            table.setItem(
                row,
                4,
                QTableWidgetItem("Create on KeSMIS" if is_create else "Update on KeSMIS"),
            )
            table.setItem(
                row,
                5,
                QTableWidgetItem(sync_status_for_selection(match, selected)),
            )

        table.resizeColumnsToContents()
        matching_layout.addWidget(table)

        if no_match_count:
            note = QLabel(
                f"{no_match_count} feature(s) did not intersect any KeSMIS settlement. "
                "They will be created on the server with a new code."
            )
            note.setWordWrap(True)
            note.setStyleSheet("color: #8a4b00;")
            matching_layout.addWidget(note)

        tabs.addTab(matching_tab, "Code Matching")

        # Tab 2: field mapping
        mapping_tab = QWidget()
        mapping_layout = QVBoxLayout(mapping_tab)
        mapping_layout.setContentsMargins(8, 8, 8, 8)
        mapping_label = QLabel("Map local layer fields to KeSMIS settlement API attributes:")
        mapping_label.setWordWrap(True)
        mapping_layout.addWidget(mapping_label)

        mapping_table = QTableWidget(len(mapping_table_data), 3)
        mapping_table.setHorizontalHeaderLabels(["Layer Field", "API Field", "Match Score"])
        mapping_table.horizontalHeader().setStretchLastSection(True)
        mapping_combos = {}
        for row, (layer_field, matched_api_field, score) in enumerate(mapping_table_data):
            mapping_table.setItem(row, 0, QTableWidgetItem(layer_field))
            combo = SearchableComboBox()
            combo.addItems(api_fields)
            combo.setCurrentText(matched_api_field if matched_api_field else "-")
            mapping_table.setCellWidget(row, 1, combo)
            mapping_table.setItem(row, 2, QTableWidgetItem(score))
            mapping_combos[layer_field] = combo
        mapping_table.resizeColumnsToContents()
        mapping_layout.addWidget(mapping_table)

        tabs.addTab(mapping_tab, "Field Mapping")
        layout.addWidget(tabs, 1)

        buttons = QHBoxLayout()
        sync_button = QPushButton("Sync to KeSMIS")
        cancel_button = QPushButton("Cancel")
        sync_button.clicked.connect(dialog.accept)
        cancel_button.clicked.connect(dialog.reject)
        buttons.addStretch()
        buttons.addWidget(sync_button)
        buttons.addWidget(cancel_button)
        layout.addLayout(buttons)

        if dialog.exec_() != QDialog.Accepted:
            return None, field_mapping

        final_field_mapping = {}
        for layer_field, combo in mapping_combos.items():
            api_field = combo.currentText()
            final_field_mapping[layer_field] = (
                None if api_field == "-" or not api_field else api_field
            )

        resolved = []
        for row, match in enumerate(matches):
            selector_type, selector = selectors[row]
            if selector_type == "combo":
                selected = selector.currentData()
            else:
                selected = selector

            kesmis_code = selected["kesmis_code"]
            is_generated = selected.get("generated", False)
            resolved.append(
                {
                    "feature_id": match["feature_id"],
                    "local_label": match["local_label"],
                    "current_code": match["current_code"],
                    "kesmis_label": selected["kesmis_label"],
                    "kesmis_code": kesmis_code,
                    "kesmis_id": selected.get("kesmis_id"),
                    "kesmis_row": selected.get("kesmis_row"),
                    "is_generated": is_generated,
                    "status": self._settlement_sync_status(
                        match["current_code"],
                        kesmis_code,
                        is_create=is_generated,
                    ),
                }
            )
        return resolved, final_field_mapping

    def _row_code(self, row):
        for key in ("code", "pcode", "settlement_code", "Code", "PCODE"):
            if key in row.index and row[key] is not None and str(row[key]).strip():
                return str(row[key]).strip()
        return ""

    def _normalize_kesmis_code_column(self, gdf):
        if gdf.empty:
            return gdf, None

        gdf = gdf.copy()
        for col in ("code", "pcode", "settlement_code", "Code", "PCODE"):
            if col in gdf.columns and gdf[col].notna().any():
                if col != "code":
                    gdf["code"] = gdf[col]
                return gdf, col

        lower_map = {str(c).lower(): c for c in gdf.columns if c != "geometry"}
        for col in ("code", "pcode", "settlement_code"):
            if col in lower_map:
                src = lower_map[col]
                if gdf[src].notna().any():
                    gdf["code"] = gdf[src]
                    return gdf, src
        return gdf, None

    def _enrich_settlement_codes_from_ids(self, gdf, url, headers):
        if "id" not in gdf.columns:
            return gdf, None

        ids = []
        for value in gdf["id"].dropna().unique():
            try:
                ids.append(int(value))
            except (TypeError, ValueError):
                continue
        if not ids:
            return gdf, None

        code_by_id = {}
        batch_size = 200
        endpoints = [
            f"{url}/api/v1/data/many/id",
            f"{url}/api/v1/data/many/ids",
        ]
        for endpoint in endpoints:
            for start in range(0, len(ids), batch_size):
                batch = ids[start:start + batch_size]
                try:
                    response = requests.post(
                        endpoint,
                        headers=headers,
                        json={"model": "settlement", "ids": batch},
                        timeout=60,
                    )
                    if response.status_code != 200:
                        continue
                    payload = response.json()
                    records = payload.get("data", payload if isinstance(payload, list) else [])
                    for rec in records:
                        rec_id = rec.get("id")
                        rec_code = rec.get("code") or rec.get("pcode")
                        if rec_id is not None and rec_code is not None and str(rec_code).strip():
                            code_by_id[int(rec_id)] = str(rec_code).strip()
                except Exception as e:
                    self.log_message(f"Settlement code lookup failed at {endpoint}: {e}")
            if code_by_id:
                break

        if not code_by_id:
            return gdf, None

        gdf = gdf.copy()

        def lookup_code(value):
            if value is None or str(value).strip() == "":
                return None
            try:
                return code_by_id.get(int(value))
            except (TypeError, ValueError):
                return None

        gdf["code"] = gdf["id"].apply(lookup_code)
        return gdf, "code"

    def _fetch_kesmis_settlements_gdf(self, url, headers):
        fetch_attempts = [
            ("geo/minimal", f"{url}/api/v1/data/geo/minimal", {"model": "settlement"}),
            ("geo/full", f"{url}/api/v1/data/geo", {"model": "settlement"}),
        ]
        last_columns = []
        last_error = None

        for label, endpoint, params in fetch_attempts:
            try:
                response = requests.get(endpoint, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                gdf = self._build_gdf_from_geojson(response.json())
                if gdf.empty:
                    last_error = f"KeSMIS {label} returned no settlement features."
                    continue

                last_columns = [c for c in gdf.columns if c != "geometry"]
                gdf, code_field = self._normalize_kesmis_code_column(gdf)
                if not code_field and "id" in gdf.columns:
                    gdf, code_field = self._enrich_settlement_codes_from_ids(gdf, url, headers)

                if code_field:
                    gdf = gdf[gdf["code"].notna() & (gdf["code"].astype(str).str.strip() != "")]
                    if gdf.empty:
                        last_error = f"KeSMIS {label} returned settlements but no usable code values."
                        continue
                    self.log_message(
                        f"Loaded {len(gdf)} KeSMIS settlement(s) from {label} using '{code_field}'."
                    )
                    return gdf

                last_error = (
                    f"KeSMIS {label} returned no code field. Available columns: {', '.join(last_columns)}"
                )
                self.log_message(last_error)
            except Exception as e:
                last_error = str(e)
                self.log_message(f"KeSMIS {label} settlement fetch failed: {e}")

        if last_columns:
            raise ValueError(
                "KeSMIS settlement data has no code field. "
                f"Available columns: {', '.join(last_columns)}"
            )
        raise ValueError(last_error or "Could not load settlement data from KeSMIS.")

    def _build_settlement_upsert_features(self, layer_gdf, resolved, field_mapping, entity, wards_gdf=None):
        """Build KeSMIS upsert payloads from resolved settlement matches and field mapping."""
        allowed_fields = self._get_writable_settlement_api_fields(entity)
        entity_attrs = self._entity_attr_lookup(entity)
        features = []
        for match in resolved:
            if not match.get("kesmis_code"):
                continue

            feature_rows = layer_gdf[layer_gdf["_qgis_fid"] == match["feature_id"]]
            if feature_rows.empty:
                continue
            row = feature_rows.iloc[0]
            kesmis_row = match.get("kesmis_row") or {}

            feature = {"code": str(match["kesmis_code"]).strip()}

            for layer_field, api_field in field_mapping.items():
                if not api_field or api_field not in allowed_fields:
                    continue
                if layer_field not in row.index:
                    continue
                value = self._coerce_settlement_api_value(
                    api_field, row[layer_field], entity_attrs
                )
                if value is not None:
                    feature[api_field] = value

            for key in self.pcode_fields:
                if key in feature:
                    continue
                if kesmis_row.get(key) is not None:
                    try:
                        feature[key] = int(kesmis_row[key])
                    except (TypeError, ValueError):
                        pass
                elif key in row.index and self._has_meaningful_value(row[key]):
                    try:
                        feature[key] = int(row[key])
                    except (TypeError, ValueError):
                        pass

            if match.get("is_generated") or "ward_id" not in feature:
                if wards_gdf is not None:
                    parent_ids = self._lookup_ward_parent_ids(row.geometry, wards_gdf)
                    for key, value in parent_ids.items():
                        if key not in feature:
                            feature[key] = value

            geom = self._build_settlement_geometry(row.geometry)
            if geom is not None:
                feature["geom"] = geom
            feature["isApproved"] = True

            filtered = {"code": feature["code"], "isApproved": True}
            if feature.get("geom") is not None:
                filtered["geom"] = feature["geom"]
            for key, value in feature.items():
                if key in ("code", "geom", "isApproved"):
                    continue
                if key in allowed_fields and self._has_meaningful_value(value):
                    filtered[key] = self._convert_to_serializable(value)
            features.append(filtered)
        return features

    def _submit_upsert_batches(
        self,
        model,
        features,
        dry_run=False,
        dry_run_limit=None,
        progress_start=0,
        progress_end=100,
        manage_visibility=True,
    ):
        """Submit features to KeSMIS import/upsert in batches."""
        if not features:
            return 0, 0, 0, []

        if dry_run and dry_run_limit is not None:
            features = features[:dry_run_limit]

        url = self.server_url
        headers = {
            "Authorization": f"Bearer {self.token}",
            "x-access-token": self.token,
        }
        batch_size = 100
        total = len(features)
        all_inserted = all_updated = all_failed = 0
        all_errors = []
        progress_span = max(progress_end - progress_start, 1)

        if manage_visibility:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(progress_start)
            self.progress_bar.setVisible(True)
        elif progress_start == 0:
            self.progress_bar.setValue(0)

        for start in range(0, total, batch_size):
            batch = [
                self._convert_to_serializable(feature)
                for feature in features[start:start + batch_size]
            ]
            batch_num = start // batch_size + 1
            action = "Validating batch" if dry_run else "Submitting batch"
            self.log_message(
                f"{action} {batch_num} ({start + 1}–{min(start + batch_size, total)} of {total})…"
            )
            if not manage_visibility:
                self._update_settlement_sync_progress(
                    message=f"{action} {batch_num} ({start + 1}–{min(start + batch_size, total)} of {total})...",
                )
            try:
                payload = {"model": model, "data": batch}
                if dry_run:
                    payload["dryRun"] = True
                resp = requests.post(
                    f"{url}/api/v1/data/import/upsert",
                    json=payload,
                    headers=headers,
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                all_inserted += data.get("insertedCount", 0)
                all_updated += data.get("updatedCount", 0)
                all_failed += data.get("failedCount", 0)
                all_errors.extend(data.get("errors", []))
            except requests.HTTPError as e:
                detail = _format_import_http_error(
                    e.response,
                    str(e),
                    format_error_lines=self._format_import_error_lines,
                )
                if batch:
                    sample_keys = ", ".join(sorted(batch[0].keys()))
                    self.log_message(f"Batch {batch_num} payload keys: {sample_keys}")
                self.log_message(f"Batch {batch_num} failed entirely: {detail}")
                all_failed += len(batch)
            except Exception as e:
                self.log_message(f"Batch {batch_num} failed entirely: {e}")
                all_failed += len(batch)

            percent = progress_start + int((start + len(batch)) / total * progress_span)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(min(percent, progress_end))
            QApplication.processEvents()

        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(progress_end)
        QApplication.processEvents()
        if manage_visibility:
            self.progress_bar.setVisible(False)
        return all_inserted, all_updated, all_failed, all_errors

    def _apply_settlement_code_matches(self, layer, matches):
        from qgis.core import edit

        transferable = [
            m for m in matches
            if m["kesmis_code"] and (
                m["status"].startswith("Will update on KeSMIS")
                or m["status"] == "Will create on KeSMIS"
                or m["status"] in (
                    "Will copy code",
                    "Will replace code",
                    "Already matches",
                    "Will generate new code",
                )
            )
        ]
        if not transferable:
            return 0, 0, 0, 0

        filled = 0
        updated = 0
        unchanged = 0
        generated = 0
        sync_stamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        with edit(layer):
            code_idx = self._ensure_code_field_index(layer)
            sync_idx = self._ensure_sync_field_index(layer)
            for match in transferable:
                feature_id = match["feature_id"]
                kesmis_code = match["kesmis_code"]
                current_code = match["current_code"]
                is_generated = match.get("is_generated", False)
                if is_generated:
                    generated += 1
                if not current_code:
                    layer.changeAttributeValue(feature_id, code_idx, kesmis_code)
                    filled += 1
                elif current_code == kesmis_code:
                    unchanged += 1
                else:
                    layer.changeAttributeValue(feature_id, code_idx, kesmis_code)
                    updated += 1
                if sync_idx >= 0:
                    layer.changeAttributeValue(feature_id, sync_idx, sync_stamp)

        return filled, updated, unchanged, generated

    def _ensure_sync_field_index(self, layer):
        """Ensure the kesmis_sync marker field exists; must be called inside an edit session."""
        for i, f in enumerate(layer.fields()):
            if f.name().lower() == self.SETTLEMENT_SYNC_FIELD:
                return i
        from qgis.core import QgsField
        layer.addAttribute(QgsField(self.SETTLEMENT_SYNC_FIELD, QVariant.String))
        layer.updateFields()
        return layer.fields().indexOf(self.SETTLEMENT_SYNC_FIELD)

    def _ensure_code_field_index(self, layer):
        fields = layer.fields()
        field_names = [f.name() for f in fields]
        lower_to_index = {name.lower(): idx for idx, name in enumerate(field_names)}
        if "code" in lower_to_index:
            idx = lower_to_index["code"]
            if field_names[idx] != "code":
                layer.renameAttribute(idx, "code")
                layer.updateFields()
            return layer.fields().indexOf("code")
        from qgis.core import QgsField
        layer.addAttribute(QgsField("code", QVariant.String))
        layer.updateFields()
        return layer.fields().indexOf("code")

    def _build_gdf_from_layer(self, layer, include_fid=False):
        srid = layer.crs().postgisSrid() or 4326
        features = []
        for f in layer.getFeatures():
            props = {
                field: self._convert_to_serializable(f[field])
                for field in [fld.name() for fld in layer.fields()]
            }
            if include_fid:
                props["_qgis_fid"] = f.id()
            features.append(
                {
                    "type": "Feature",
                    "geometry": json.loads(f.geometry().asJson()) if f.geometry() else None,
                    "properties": props,
                }
            )
        gdf = gpd.GeoDataFrame.from_features(features, crs=f"EPSG:{srid}")
        if gdf.empty:
            return gdf
        gdf["geometry"] = gdf["geometry"].apply(self.to_2d)
        return gdf.to_crs(epsg=4326)

    def _build_gdf_from_geojson(self, geojson):
        records = []
        for feat in geojson.get("features", []):
            geom_dict = feat.get("geometry")
            if not geom_dict:
                continue
            try:
                geom = validate_and_repair_geometry(shape(geom_dict))
            except Exception:
                continue
            if geom is None or geom.is_empty:
                continue
            props = feat.get("properties")
            if props is None:
                props = {k: v for k, v in feat.items() if k not in ("geometry", "type")}
            record = dict(props)
            record["geometry"] = geom
            records.append(record)
        if not records:
            return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
        gdf.set_crs(epsg=4326, inplace=True)
        return gdf

    def sync_settlement_codes_for_selection(self):
        layer = self.settlement_layer_combo.currentData()
        if not layer:
            settlement_layers = self._get_settlement_layers()
            if not settlement_layers:
                QMessageBox.information(
                    self,
                    "No Settlement Layers",
                    "No settlement data layers were detected in the project.\n\n"
                    "Layers are treated as settlements when their name contains 'settlement' "
                    "(excluding reference layers ending in ' Boundaries').",
                )
            else:
                QMessageBox.information(
                    self,
                    "Select Settlement Layer",
                    "Choose the settlement layer to match against KeSMIS before syncing codes.",
                )
            return

        if not self.token:
            QMessageBox.warning(
                self,
                "Login Required",
                "Log in to KeSMIS before syncing settlements with the server.",
            )
            return

        self._sync_settlement_codes_from_kesmis(layer)

    def _sync_settlement_codes_from_kesmis(self, layer):
        layer_name = layer.name()
        settlement_entity = self._get_settlement_entity()
        if not settlement_entity:
            QMessageBox.warning(
                self,
                "Settlement Model Unavailable",
                "Could not find the settlement entity on KeSMIS.\n\n"
                "Wait for entities to load after login, then try again.",
            )
            return

        self.log_message(f"Fetching KeSMIS settlements and matching '{layer_name}'...")
        self.sync_settlement_codes_button.setEnabled(False)
        self._start_settlement_sync_progress("Fetching KeSMIS settlements...")

        url = self.server_url
        headers = {"Authorization": f"Bearer {self.token}", "x-access-token": self.token}
        try:
            settlements_gdf = self._fetch_kesmis_settlements_gdf(url, headers)
            self._update_settlement_sync_progress(20, "Mapping layer fields...")

            exclude_fields = {self.SETTLEMENT_SYNC_FIELD}
            field_mapping, mapping_table_data = self._auto_match_fields(
                layer, settlement_entity, exclude_fields
            )
            mapped_count = sum(1 for api_field in field_mapping.values() if api_field)
            self.log_message(
                f"Auto-mapped {mapped_count} layer field(s) to settlement API attributes."
            )
            self._update_settlement_sync_progress(25, "Matching local settlements to KeSMIS...")

            matches, layer_gdf = self._compute_settlement_matches(
                layer,
                settlements_gdf,
                progress_callback=self._update_settlement_sync_progress,
            )
            if not matches:
                QMessageBox.warning(self, "No Features", f"Layer '{layer_name}' contains no features.")
                return

            self._update_settlement_sync_progress(65, "Review matches...")
            resolved, field_mapping = self._resolve_settlement_matches(
                layer_name,
                matches,
                settlement_entity,
                field_mapping,
                mapping_table_data,
            )
            if not resolved:
                self.log_message("Settlement sync cancelled.")
                return

            self._update_settlement_sync_progress(70, "Loading ward boundaries...", indeterminate=True)
            wards_gdf = self._fetch_kesmis_geo_gdf("ward", url, headers)
            self._update_settlement_sync_progress(80, "Preparing records for upload...")

            features = self._build_settlement_upsert_features(
                layer_gdf,
                resolved,
                field_mapping,
                settlement_entity,
                wards_gdf=wards_gdf,
            )
            if not features:
                QMessageBox.warning(
                    self,
                    "Nothing To Sync",
                    "No settlement features could be prepared for submission.",
                )
                return

            is_dry_run = self.dry_run_checkbox.isChecked()
            dry_run_limit = self.dry_run_spinbox.value() if is_dry_run else None
            if is_dry_run:
                self.log_message(
                    f"DRY RUN: validating up to {dry_run_limit} settlement record(s) on KeSMIS."
                )

            inserted, updated, failed, errors = self._submit_upsert_batches(
                settlement_entity["model"],
                features,
                dry_run=is_dry_run,
                dry_run_limit=dry_run_limit,
                progress_start=85,
                progress_end=98,
                manage_visibility=False,
            )

            if is_dry_run:
                summary = (
                    f"Settlement sync dry run for '{layer_name}'.\n\n"
                    f"Would insert: {inserted}\n"
                    f"Would update: {updated}\n"
                    f"Failed validation: {failed}"
                )
                dialog_title = "Settlement Sync Dry Run"
                if errors:
                    summary += "\n\nErrors:\n" + "\n".join(self._format_import_error_lines(errors))
                    QMessageBox.warning(self, dialog_title, summary)
                else:
                    QMessageBox.information(self, dialog_title, summary)
                self.log_message(summary.replace("\n", " "))
                return

            if failed and not inserted and not updated:
                summary = (
                    f"Settlement sync failed for '{layer_name}'.\n\n"
                    f"Failed: {failed}\n\n"
                    "Check the log for the server error message and payload field list."
                )
                if errors:
                    summary += "\n\nErrors:\n" + "\n".join(self._format_import_error_lines(errors))
                self.log_message(summary.replace("\n", " "))
                QMessageBox.critical(self, "Settlement Sync Failed", summary)
                return

            filled, local_updated, unchanged, generated = self._apply_settlement_code_matches(
                layer, resolved
            )
            self._update_settlement_sync_progress(100, "Settlement sync complete.")
            summary = (
                f"Settlement sync finished for '{layer_name}'.\n\n"
                f"KeSMIS inserted: {inserted}\n"
                f"KeSMIS updated: {updated}\n"
                f"KeSMIS failed: {failed}\n\n"
                f"Local codes added: {filled}\n"
                f"Local codes replaced: {local_updated}\n"
                f"Local codes unchanged: {unchanged}\n"
                f"New local codes (created): {generated}\n\n"
                f"Synced features are marked in the '{self.SETTLEMENT_SYNC_FIELD}' field."
            )
            self.log_message(summary.replace("\n", " "))
            if errors:
                summary += "\n\nErrors:\n" + "\n".join(self._format_import_error_lines(errors))
                QMessageBox.warning(self, "Settlement Sync Complete", summary)
            else:
                QMessageBox.information(self, "Settlement Sync Complete", summary)

            self.populate_layers()
            if self.layer_combo.currentData():
                self.reset_data()
        except Exception as e:
            self.log_message(f"Failed to sync settlements for '{layer_name}': {e}")
            QMessageBox.critical(
                self,
                "Settlement Sync Failed",
                f"Could not sync settlements for '{layer_name}':\n{e}",
            )
        finally:
            self._finish_settlement_sync_progress()
            self._update_code_guidance()

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
        """Convert QVariant, numpy/pandas, and other types to JSON-serializable values."""
        if value is None:
            return None
        if isinstance(value, QVariant):
            if value.isNull():
                return None
            return self._convert_to_serializable(value.toPyObject())
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        try:
            import numpy as np
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                number = float(value)
                if number != number or number in (float("inf"), float("-inf")):
                    return None
                return number
            if isinstance(value, np.bool_):
                return bool(value)
        except ImportError:
            pass
        if hasattr(value, "item") and not isinstance(value, (bytes, str)):
            try:
                return self._convert_to_serializable(value.item())
            except (ValueError, AttributeError, TypeError):
                pass
        if isinstance(value, (list, tuple)):
            return [self._convert_to_serializable(item) for item in value]
        if isinstance(value, dict):
            return {k: self._convert_to_serializable(v) for k, v in value.items()}
        if isinstance(value, (int, float, str, bool)):
            if isinstance(value, float) and (
                value != value or value in (float("inf"), float("-inf"))
            ):
                return None
            return value
        return str(value)
 
    def reset_data(self):
        """
        Clear data when the layer changes and re‐fetch parent IDs if a parent is selected.
        Immediately projects to EPSG:4326 and maintains that projection.
        """
        self.pcode_entity_data = {}
        self.valid_feature_indices = []
        self.field_mapping = {}
        self.mapping_table.setRowCount(0)
        self.submit_button.setEnabled(False)

        layer = self.layer_combo.currentData()
        if layer:
            if not self._ensure_layer_code_ready(layer):
                self.gdf = None
                self.log_message("Layer selection cleared because code requirements were not met.")
                self._update_code_guidance()
                return

            try:
                # 1) Grab the layer's native SRID
                srid = layer.crs().postgisSrid()
                self.log_message(f"Layer CRS SRID detected: {srid}")

                # 2) Build a list of GeoJSON‐like feature dicts
                features = [
                    {
                        "type": "Feature",
                        "geometry": json.loads(f.geometry().asJson()) if f.geometry() else None,
                        "properties": {
                            field: self._convert_to_serializable(f[field])
                            for field in [fld.name() for fld in layer.fields()]
                        }
                    }
                    for f in layer.getFeatures()
                ]

                # 3) Create GeoDataFrame with layer's CRS
                self.gdf = gpd.GeoDataFrame.from_features(features, crs=f"EPSG:{srid}")
                if self.gdf.empty:
                    self.gdf = None
                    self.log_message("Selected layer contains no features.")
                    QMessageBox.warning(self, "No Features", "The selected layer contains no features.")
                    return

                # 4) Immediately convert to 2D and reproject to EPSG:4326
                try:
                    self.gdf['geometry'] = self.gdf['geometry'].apply(self.to_2d)
                    self.gdf = self.gdf.to_crs(epsg=4326)
                    self.log_message("Layer immediately reprojected to EPSG:4326.")
                except Exception as e:
                    self.log_message(f"Error reprojecting to EPSG:4326: {str(e)}")
                    QMessageBox.critical(self, "Projection Error", f"Failed to reproject layer: {str(e)}")
                    return

                # 3) Set geojson column from the already reprojected geometry
                self.gdf["geojson"] = self.gdf.geometry.apply(
                    lambda geom: geom.__geo_interface__ if geom is not None else None
                )

                self.log_message("Layer loaded and prepared in EPSG:4326.")
            except Exception as e:
                self.gdf = None
                self.log_message(f"Error creating GeoDataFrame: {str(e)}")
                QMessageBox.critical(self, "Error", f"Failed to create GeoDataFrame: {str(e)}")
                return
        else:
            self.gdf = None
            self.log_message("No valid layer selected.")

        self.log_message("Cleared pcode data and field mappings due to layer change.")
        self._update_code_guidance()

        if self.parent_combo.currentText():
            self.start_fetch_pcode_data()

    def _prompt_parent_geojson_source(self, parent_entity, layer_name, local_filepath):
        """Ask how parent boundaries should be loaded before spatial matching."""
        layer_loaded = any(
            lyr.name() == layer_name for lyr in QgsProject.instance().mapLayers().values()
        )
        local_exists = os.path.exists(local_filepath)

        msg = QMessageBox(self)
        msg.setWindowTitle("Load Parent Boundaries")
        msg.setIcon(QMessageBox.Question)
        msg.setText(f"Load parent boundaries for '{parent_entity}'?")
        status_lines = [
            "These boundaries are used to match your features to the correct parent entity "
            "during Import.",
            "",
            "Current status:",
        ]
        if layer_loaded:
            status_lines.append(f"- Layer '{layer_name}' is already loaded in QGIS.")
        else:
            status_lines.append(f"- Layer '{layer_name}' is not loaded in QGIS.")
        if local_exists:
            status_lines.append(f"- Cached local file found:\n  {local_filepath}")
        else:
            status_lines.append("- No cached local GeoJSON file was found.")
        status_lines.append("")
        status_lines.append(
            "Choose how to load parent boundaries. Downloading from the server is recommended "
            "so spatial matching uses the latest data."
        )
        msg.setInformativeText("\n".join(status_lines))

        download_btn = msg.addButton("Download fresh from server", QMessageBox.AcceptRole)
        local_btn = msg.addButton("Use existing local file", QMessageBox.ActionRole)
        cancel_btn = msg.addButton("Cancel", QMessageBox.RejectRole)
        msg.setDefaultButton(download_btn)
        if not local_exists:
            local_btn.setEnabled(False)

        msg.exec_()
        clicked = msg.clickedButton()
        if clicked == cancel_btn:
            return None
        if clicked == local_btn:
            return "local"
        return "download"

    def start_fetch_pcode_data(self):
        """Start fetching pcode data in a background thread."""
        if not self.layer_combo.currentData() or not self.parent_combo.currentText():
            return

        parent_entity = self.parent_combo.currentText()
        layer_name = f"{parent_entity.capitalize()} Boundaries"

        documents_path = os.path.expanduser("~/Documents")
        odk_data_path = os.path.join(documents_path, "ODK_Data")
        os.makedirs(odk_data_path, exist_ok=True)
        filename = f"parent({parent_entity}).geojson"
        fallback_filepath = os.path.join(odk_data_path, filename)

        source_choice = self._prompt_parent_geojson_source(
            parent_entity, layer_name, fallback_filepath
        )
        if source_choice is None:
            self.log_message("Parent boundary load cancelled.")
            return

        self.pcode_entity_data = {}
        self.valid_feature_indices = []
        if source_choice == "local":
            self.log_message(
                f"Starting pcode data fetch for parent entity '{parent_entity}' "
                f"(using existing local GeoJSON file)"
            )
        else:
            self.log_message(
                f"Starting pcode data fetch for parent entity '{parent_entity}' "
                f"(downloading parent GeoJSON from server)"
            )

        self.layer_combo.setEnabled(False)
        self.parent_combo.setEnabled(False)
        self.entity_combo.setEnabled(False)
        self.submit_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.worker = Worker(
            self.layer_combo.currentData(),
            self.parent_combo.currentText(),
            self.server_url,
            self.token
        )
        self.worker.local_fallback_filepath = fallback_filepath
        self.worker.use_local_file = source_choice == "local"
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

    def populate_settlement_layers(self):
        """Populate settlement layer dropdown without auto-selecting a layer."""
        current_layer = self.settlement_layer_combo.currentData()
        current_id = current_layer.id() if current_layer else None
        self.settlement_layer_combo.blockSignals(True)
        self.settlement_layer_combo.clear()
        self.settlement_layer_combo.addItem("— Select settlement layer —", None)
        settlement_layers = self._get_settlement_layers()
        for layer in settlement_layers:
            self.settlement_layer_combo.addItem(layer.name(), layer)

        selected_index = 0
        if current_id is not None:
            for i in range(self.settlement_layer_combo.count()):
                layer = self.settlement_layer_combo.itemData(i)
                if layer and layer.id() == current_id:
                    selected_index = i
                    break
        self.settlement_layer_combo.setCurrentIndex(selected_index)
        self.settlement_layer_combo.setEnabled(bool(settlement_layers))
        self.settlement_layer_combo.blockSignals(False)

    def populate_layers(self):
        """Populate available layers from QGIS canvas without auto-selecting a layer."""
        current_layer = self.layer_combo.currentData()
        current_id = current_layer.id() if current_layer else None
        self.layer_combo.blockSignals(True)
        self.layer_combo.clear()
        self.layer_combo.addItem("— Select layer —", None)
        vector_layers = [
            layer for layer in QgsProject.instance().mapLayers().values()
            if isinstance(layer, QgsVectorLayer)
        ]
        for layer in vector_layers:
            self.layer_combo.addItem(layer.name(), layer)

        selected_index = 0
        if current_id is not None:
            for i in range(self.layer_combo.count()):
                layer = self.layer_combo.itemData(i)
                if layer and layer.id() == current_id:
                    selected_index = i
                    break
        self.layer_combo.setCurrentIndex(selected_index)
        self.layer_combo.setEnabled(bool(vector_layers))
        self.layer_combo.blockSignals(False)

        self.populate_settlement_layers()
        self._update_code_guidance()
        if not self.layer_combo.currentData():
            self.reset_data()

    def fetch_entities(self, base_url):
        """Fetch entities from API and populate the entity combo box."""
        try:
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-access-token": self.token
            }
            response = requests.get(f"{base_url}/api/v1/models/list", headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                self.api_entities = data.get("models", [])
                # Sort entities by model name
                self.api_entities.sort(key=lambda e: e.get("model", "").lower())
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

    def _on_entity_activated(self, index):
        """Run field matching only after the user confirms an entity choice."""
        if getattr(self.entity_combo, "_updating", False):
            return
        if index <= 0:
            return
        self.match_fields()

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
        api_fields.sort(key=lambda x: x.lower())

        self.mapping_table.blockSignals(True)
        self.mapping_table.setRowCount(0)
        for field, matched_api_field, score in table_data:
            row = self.mapping_table.rowCount()
            self.mapping_table.insertRow(row)
            self.mapping_table.setItem(row, 0, QTableWidgetItem(field))

            combo = SearchableComboBox()
            combo.addItems(api_fields)  # Add sorted API field list
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
        """Sanitize JSON values to handle NaN, infinity, and numpy/pandas scalars."""
        return self._convert_to_serializable(value)

    def _format_import_error_lines(self, errors, limit=15):
        """Format API import errors for display in logs and message boxes."""
        lines = []
        for err in errors[:limit]:
            item = err.get("item") or {}
            code = item.get("code") or item.get("id") or "<unknown>"
            error = err.get("error") or "Error"
            detail = err.get("detail") or ""
            line = f"• {code}: {error}"
            if detail:
                line += f" — {detail}"
            lines.append(line)
        if len(errors) > limit:
            lines.append(f"... and {len(errors) - limit} more error(s)")
        return lines

    def submit_features(self):
        """Submit features to API in batches of 100 with progress updates."""
        try:
            layer = self.layer_combo.currentData()
            url = self.server_url
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
                # Add isApproved property
                feature["isApproved"] = True
                features.append(feature)

            if not features:
                self.log_message("No features with valid parent IDs to submit.")
                QMessageBox.warning(self, "No Valid Features", "No features with valid parent IDs were found for submission.")
                return

            # Check for dry run mode
            is_dry_run = self.dry_run_checkbox.isChecked()
            dry_run_limit = self.dry_run_spinbox.value() if is_dry_run else None
            
            if is_dry_run:
                original_count = len(features)
                features = features[:dry_run_limit]
                self.log_message(
                    f"DRY RUN MODE: Validating {len(features)} of {original_count} features "
                    f"(limit: {dry_run_limit}). No records will be saved."
                )

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
                action = "Validating batch" if is_dry_run else "Submitting batch"
                self.log_message(f"{action} {batch_num} ({start+1}–{min(start+batch_size, total)} of {total})…")
                try:
                    payload = {"model": entity["model"], "data": batch}
                    if is_dry_run:
                        payload["dryRun"] = True
                    resp = requests.post(
                        f"{url}/api/v1/data/import/upsert",
                        json=payload,
                        headers=headers,
                        timeout=30
                    )
                    resp.raise_for_status()
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

            if is_dry_run:
                summary = (
                    f"DRY RUN COMPLETE (nothing saved):\n\n"
                    f"Would insert: {all_inserted}\n"
                    f"Would update: {all_updated}\n"
                    f"Failed: {all_failed} (out of {dry_run_limit} test records)"
                )
                dialog_title = "Dry Run Complete"
            else:
                summary = f"Done: {all_inserted} inserted, {all_updated} updated, {all_failed} failed."
                dialog_title = "Import Complete"

            self.log_message(summary.replace("\n", " "))
            for err in all_errors:
                code = (err.get("item") or {}).get("code", "<unknown>")
                self.log_message(f"Error {code}: {err.get('error')} — {err.get('detail')}")

            if is_dry_run and all_errors:
                summary += "\n\nErrors:\n" + "\n".join(self._format_import_error_lines(all_errors))
                QMessageBox.warning(self, dialog_title, summary)
            else:
                QMessageBox.information(self, dialog_title, summary)

        except Exception as e:
            self.log_message(f"Error submitting features: {e}")
            QMessageBox.critical(self, "Error", str(e))

        finally:
            self.submit_button.setEnabled(True)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setVisible(False)

    def update_submit_button_text(self):
        """Update submit button text based on dry run mode."""
        if self.dry_run_checkbox.isChecked():
            limit = self.dry_run_spinbox.value()
            self.submit_button.setText(f"Validate on KeSMIS (Dry Run: {limit} records)")
        else:
            self.submit_button.setText("Submit Data to KeSMIS")
    
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