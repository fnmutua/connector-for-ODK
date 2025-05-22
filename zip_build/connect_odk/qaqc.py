import sys
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import LineString, MultiLineString, Polygon, MultiPolygon
from shapely.strtree import STRtree
from fpdf import FPDF
import pyproj
from fuzzywuzzy import process
from PyQt5.QtWidgets import (
    QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, QLineEdit,
    QSpinBox, QFileDialog, QHBoxLayout, QMessageBox, QGroupBox,
    QTextEdit, QScrollArea, QGridLayout, QWidget, QApplication
)
from PyQt5.QtCore import Qt
from qgis.core import (
    QgsProject, QgsVectorLayer, QgsFeatureRequest, QgsFields, QgsFeature, QgsWkbTypes
)

class ProcessGDBDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Quality Assurance / Quality Control")
        self.setFixedSize(1000, 800)

        # Set the Excel file path relative to the plugin directory
        plugin_dir = os.path.dirname(__file__)
        #self.excel_file = os.path.join(plugin_dir, "data", "dictionary.xlsx")
        self.excel_file = os.path.join(plugin_dir, "dictionary.xlsx")
        
        # Check if the Excel file exists
        if not os.path.exists(self.excel_file):
            self.excel_file = None
            print(f"Error: Excel file not found at {self.excel_file}")

        # Main layout for the dialog
        main_layout = QVBoxLayout()

        # Top label
        self.label = QLabel("Select a GeoDatabase, output folder, and set criteria")
        main_layout.addWidget(self.label)

        # Horizontal layout for select inputs (left) and parameters (right)
        top_hbox = QHBoxLayout()

        # Left side: Select inputs panel (50% width)
        select_inputs_box = QGroupBox("Select Inputs")
        select_inputs_layout = QVBoxLayout()
        select_inputs_layout.setSpacing(10)

        # GeoDatabase selection
        self.gdb_button = QPushButton("Select GeoDatabase")
        self.gdb_button.clicked.connect(self.select_gdb)
        self.gdb_label = QLabel("No GeoDatabase selected")
        self.gdb_label.setStyleSheet("font-style: italic; color: gray;")
        select_inputs_layout.addWidget(self.gdb_button)
        select_inputs_layout.addWidget(self.gdb_label)

        # Output folder selection
        self.output_button = QPushButton("Select Output Folder")
        self.output_button.setEnabled(False)
        self.output_button.clicked.connect(self.select_output_folder)
        self.output_label = QLabel("No output folder selected")
        self.output_label.setStyleSheet("font-style: italic; color: gray;")
        select_inputs_layout.addWidget(self.output_button)
        select_inputs_layout.addWidget(self.output_label)

        # Add stretch to push content up
        select_inputs_layout.addStretch()
        select_inputs_box.setLayout(select_inputs_layout)

        # Right side: Parameters (50% width)
        parameters_box = QGroupBox("Set Parameters")
        parameters_layout = QVBoxLayout()

        # Angular Parameters Group
        params_group = QGroupBox("Linear Feature Parameters")
        angular_params_layout = QVBoxLayout()
        self.min_angle_spinbox = QSpinBox()
        self.min_angle_spinbox.setRange(0, 360)
        self.min_angle_spinbox.setPrefix("Min Angle: ")
        self.min_angle_spinbox.setValue(1)
        self.max_angle_spinbox = QSpinBox()
        self.max_angle_spinbox.setRange(0, 360)
        self.max_angle_spinbox.setPrefix("Max Angle: ")
        self.max_angle_spinbox.setValue(45)
        angular_params_layout.addWidget(self.min_angle_spinbox)
        angular_params_layout.addWidget(self.max_angle_spinbox)
        params_group.setLayout(angular_params_layout)
        parameters_layout.addWidget(params_group)

        # Length Parameters Group
        length_group = QGroupBox("Length Parameters")
        length_params_layout = QVBoxLayout()
        self.min_length_spinbox = QSpinBox()
        self.min_length_spinbox.setRange(0, 50)
        self.min_length_spinbox.setPrefix("Min Length(m): ")
        self.min_length_spinbox.setValue(10)
        length_params_layout.addWidget(self.min_length_spinbox)
        length_group.setLayout(length_params_layout)
        parameters_layout.addWidget(length_group)

        parameters_box.setLayout(parameters_layout)

        # Add select inputs and parameters to top_hbox with equal stretch
        top_hbox.addWidget(select_inputs_box, 1)
        top_hbox.addWidget(parameters_box, 1)
        main_layout.addLayout(top_hbox)

        # Layers selection (100% width)
        self.layer_selection_box = QGroupBox("Select Layers")
        self.layer_selection_layout = QGridLayout()
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFixedHeight(150)
        self.scroll_widget = QWidget()
        self.scroll_widget.setLayout(self.layer_selection_layout)
        self.scroll_area.setWidget(self.scroll_widget)
        self.layer_selection_box.setLayout(QVBoxLayout())
        self.select_all_checkbox = QCheckBox("Select All")
        self.select_all_checkbox.setEnabled(False)
        self.select_all_checkbox.stateChanged.connect(self.toggle_select_all)
        self.layer_selection_box.layout().addWidget(self.select_all_checkbox)
        self.layer_selection_box.layout().addWidget(self.scroll_area)
        main_layout.addWidget(self.layer_selection_box)

        # Processing buttons (100% width)
        button_layout = QHBoxLayout()
        button_box = QGroupBox("Processing Options")
        button_box.setLayout(button_layout)
        self.run_all_button = QPushButton("Run All Checks")
        self.run_all_button.setEnabled(False)
        self.run_all_button.clicked.connect(self.run_all_checks)
        button_layout.addWidget(self.run_all_button)
        main_layout.addWidget(button_box)

        # Log section (100% width)
        log_vbox = QVBoxLayout()
        # Clear Log button (100% width)
        clear_log_hbox = QHBoxLayout()
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)
        clear_log_hbox.addStretch()
        clear_log_hbox.addWidget(self.clear_log_button)
        clear_log_hbox.addStretch()
        log_vbox.addLayout(clear_log_hbox)
        # Log widget (100% width)
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        log_vbox.addWidget(self.log_textedit)
        main_layout.addLayout(log_vbox)

        # Progress bar and label (100% width)
        self.progress_label = QLabel("Progress: Idle")
        main_layout.addWidget(self.progress_label)
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        main_layout.addWidget(self.progress_bar)
        self.progress_bar.hide()
        self.progress_label.hide()

        # PDF and folder links (100% width)
        self.pdf_link_label = QLabel("PDF Report: <a href='#'>Open Report</a>")
        self.pdf_link_label.setOpenExternalLinks(True)
        self.pdf_link_label.setStyleSheet("color: blue; text-decoration: underline;")
        self.pdf_link_label.hide()
        main_layout.addWidget(self.pdf_link_label)
        self.folder_link_label = QLabel("Open Folder: <a href='#'>Open Output Folder</a>")
        self.folder_link_label.setOpenExternalLinks(True)
        self.folder_link_label.setStyleSheet("color: blue; text-decoration: underline;")
        self.folder_link_label.hide()
        main_layout.addWidget(self.folder_link_label)

        self.setLayout(main_layout)

    def clear_log(self):
        """Clear the log widget content."""
        self.log_textedit.clear()

    def log_message(self, message):
        self.log_textedit.append(message)

    def select_gdb(self):
        gdb_path = QFileDialog.getExistingDirectory(self, "Select GeoDatabase Folder")
        if gdb_path:
            self.gdb_label.setText(gdb_path)
            self.gdb_path = gdb_path
            self.output_button.setEnabled(True)
            with fiona.Env():
                self.layers = fiona.listlayers(self.gdb_path)
                self.populate_layer_checkboxes()
                self.select_all_checkbox.setEnabled(True)

    def select_output_folder(self):
        output_folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if output_folder:
            self.output_label.setText(output_folder)
            self.output_folder = output_folder
            self.run_all_button.setEnabled(True)

    def toggle_select_all(self, state):
        for checkbox in self.layer_checkboxes:
            checkbox.setChecked(state == Qt.Checked)

    def get_selected_layers(self):
        selected_layers = []
        for checkbox in self.layer_checkboxes:
            if checkbox.isChecked():
                selected_layers.append(checkbox.text())
        return selected_layers  

    def populate_layer_checkboxes(self):
        for i in reversed(range(self.layer_selection_layout.count())):
            widget = self.layer_selection_layout.itemAt(i).widget()
            if widget is not None:
                widget.deleteLater()
        self.layer_checkboxes = []
        for i, layer in enumerate(self.layers):
            checkbox = QCheckBox(layer)
            self.layer_checkboxes.append(checkbox)
            row = i // 2
            col = i % 2
            self.layer_selection_layout.addWidget(checkbox, row, col)
        self.scroll_area.setFixedHeight(150)

    def validate_geodataframe(self, gdf):
        if "geometry" in gdf.columns and gdf.geometry.name != "geometry":
            gdf = gdf.set_geometry("geometry", inplace=False)
        return gdf

    def make_timezone_naive(self, gdf):
        for col in gdf.columns:
            if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                gdf[col] = gdf[col].dt.tz_localize(None)
        return gdf
    
    def check_duplicate_geometries(self, gdf):
        duplicate_pairs = []
        seen = {}
        for idx, geom in gdf.geometry.items():
            if geom is None or geom.is_empty:
                print(f"Skipping None or empty geometry at index {idx}")
                continue
            geom_wkt = geom.wkt
            if geom_wkt in seen:
                duplicate_pairs.append((seen[geom_wkt], idx))
            else:
                seen[geom_wkt] = idx
        if duplicate_pairs:
            duplicate_indices = {idx for pair in duplicate_pairs for idx in pair}
            return gdf.loc[list(duplicate_indices)], duplicate_pairs
        return None, None

    def check_duplicate_attributes(self, gdf):
        attr_columns = [col for col in gdf.columns if col != "geometry"]
        duplicates = gdf[gdf.duplicated(subset=attr_columns, keep=False)]
        exact_duplicates = duplicates[duplicates.duplicated(subset=attr_columns, keep="first")]
        return exact_duplicates if not exact_duplicates.empty else None

    def check_overlapping_polygons(self, gdf, tolerance=0.01):
        if gdf.crs != "EPSG:21037":
            print("Reprojecting to EPSG:21037 for accurate area calculation...")
            gdf = gdf.to_crs(epsg=21037)
        tree = STRtree(gdf.geometry)
        overlap_pairs = []
        for idx, geom in gdf.geometry.items():
            if isinstance(geom, (Polygon, MultiPolygon)):
                possible_matches = [i for i in tree.query(geom) if i != idx]
                for idx2 in possible_matches:
                    geom2 = gdf.geometry.iloc[idx2]
                    if isinstance(geom2, (Polygon, MultiPolygon)) and geom.intersects(geom2):
                        intersection = geom.intersection(geom2)
                        if not intersection.is_empty and intersection.area > tolerance:
                            overlap_area = intersection.area
                            overlap_pairs.append((idx, idx2, overlap_area))
        if overlap_pairs:
            overlap_indices = set([idx for pair in overlap_pairs for idx in [pair[0], pair[1]]])
            return gdf.iloc[list(overlap_indices)], overlap_pairs
        return None, None

    def check_sharp_turns_self_intersections(self, gdf):
        gdf = self.validate_geodataframe(gdf)
        issue_indices = set()
        issue_details = []
        lower_angle_threshold = self.min_angle_spinbox.value()
        upper_angle_threshold = self.max_angle_spinbox.value()
        for idx, geom in enumerate(gdf.geometry):
            if geom is None:
                continue
            lines = [geom] if isinstance(geom, LineString) else list(geom.geoms) if isinstance(geom, MultiLineString) else []
            for line in lines:
                if len(line.coords) < 3:
                    continue
                coords = np.array(line.coords)
                for i in range(1, len(coords) - 1):
                    p1, p2, p3 = coords[i - 1], coords[i], coords[i + 1]
                    v1 = p1 - p2
                    v2 = p3 - p2
                    dot_product = np.dot(v1, v2)
                    cross_product = np.linalg.norm(np.cross(v1, v2))
                    angle_radians = np.arctan2(cross_product, dot_product)
                    angle_degrees = np.degrees(angle_radians)
                    if lower_angle_threshold <= angle_degrees <= upper_angle_threshold:
                        issue_indices.add(idx)
                        issue_details.append((idx, "Sharp Turn", round(angle_degrees, 2), p2[0], p2[1]))
                if not line.is_simple:
                    intersections = line.intersection(line)
                    if intersections.geom_type == "Point":
                        issue_indices.add(idx)
                        issue_details.append((idx, "Self-Intersection", None, intersections.x, intersections.y))
                    elif intersections.geom_type == "MultiPoint":
                        for pt in intersections.geoms:
                            issue_indices.add(idx)
                            issue_details.append((idx, "Self-Intersection", None, pt.x, pt.y))
        if issue_indices:
            return gdf.iloc[list(issue_indices)], issue_details
        return None, None

    def check_short_linear_features(self, gdf):
        try:
            gdf = self.validate_geodataframe(gdf)
            length_threshold = self.min_length_spinbox.value()
            if gdf.crs != "EPSG:21037":
                print("Reprojecting to EPSG:21037 for accurate length calculation...")
                gdf = gdf.to_crs(epsg=21037)
            short_features = []
            for idx, geom in gdf.geometry.items():
                try:
                    if isinstance(geom, (LineString, MultiLineString)):
                        length = geom.length
                        if length < length_threshold:
                            short_features.append((gdf.iloc[idx]["feature_id"], length))
                except Exception as e:
                    print(f"Error processing geometry at index {idx}: {e}")
            if short_features:
                short_indices = [idx for idx, _ in short_features]
                return gdf.iloc[short_indices], short_features
        except Exception as e:
            print(f"An error occurred: {e}")
        return None, None

    def check_attributes(self, gdf, layer_name, unmatched_layers):
        """Validate attributes of a GeoDataFrame against specifications in the Excel file using fuzzy matching."""
        if not self.excel_file:
            self.log_message(f"No Excel file available for attribute validation of layer {layer_name}.")
            unmatched_layers.append(layer_name)
            return None, None
        try:
            xl = pd.ExcelFile(self.excel_file, engine='openpyxl')
            sheet_names = xl.sheet_names
            match = process.extractOne(layer_name, sheet_names, score_cutoff=70)
            if not match:
                self.log_message(f"No matching sheet found for layer {layer_name} (similarity score below 70).")
                unmatched_layers.append(layer_name)
                return None, None
            matched_sheet, score = match[0], match[1]
            self.log_message(f"Matched layer {layer_name} to sheet {matched_sheet} with similarity score {score}")
            specs_df = pd.read_excel(self.excel_file, sheet_name=matched_sheet, engine='openpyxl')
            required_cols = ['Attribute', 'Type']
            if not all(col in specs_df.columns for col in required_cols):
                self.log_message(f"Sheet {matched_sheet} missing required columns: {required_cols}")
                return None, None
        except Exception as e:
            self.log_message(f"Error reading Excel sheet for layer {layer_name}: {str(e)}")
            return None, None
        issue_indices = set()
        issue_details = []
        type_mapping = {
            "String": "object",
            "text": "object",
            "Integer": "int64",
            "Float": "float64",
            "decimal": "float64",
            "Boolean": "bool",
            "Date": "datetime64[ns]",
            "Array": "object"
        }
        geometry_types = ["point", "linestring", "polygon"]
        gdf_columns = [col for col in gdf.columns if col != "geometry"]
        required_fields = specs_df['Attribute'].tolist()
        missing_fields = [field for field in required_fields if field not in gdf_columns and specs_df[specs_df['Attribute'] == field]['Type'].iloc[0].lower() not in geometry_types]
        if missing_fields:
            issue_details.append((-1, "Missing Fields", f"Required fields missing: {', '.join(missing_fields)}", None, None))
        for _, spec in specs_df.iterrows():
            field = spec['Attribute']
            expected_type = spec['Type']
            max_length = spec.get('LEN', np.nan)
            valid_values = spec.get('Options', '')
            if expected_type.lower() in geometry_types:
                continue
            if field not in gdf_columns:
                issue_details.append((-1, "Missing Field", f"Field {field} is required but missing", None, None))
                continue
            actual_type = str(gdf[field].dtype)
            expected_pandas_type = type_mapping.get(expected_type, expected_type)
            if actual_type != expected_pandas_type:
                issue_details.append((-1, "Incorrect Data Type", f"Field {field} has type {actual_type}, expected {expected_pandas_type}", None, None))
            if gdf[field].isna().any():
                na_indices = gdf[gdf[field].isna()].index.tolist()
                for idx in na_indices:
                    issue_indices.add(idx)
                    issue_details.append((idx, "Missing Value", f"Field {field} is required but has missing value", None, None))
            if valid_values and isinstance(valid_values, str):
                valid_values = [v.strip() for v in valid_values.split(',') if v.strip()]
                invalid_mask = ~gdf[field].isin(valid_values) & gdf[field].notna()
                if invalid_mask.any():
                    invalid_indices = gdf[invalid_mask].index.tolist()
                    for idx in invalid_indices:
                        issue_indices.add(idx)
                        value = gdf.loc[idx, field]
                        issue_details.append((idx, "Invalid Value", f"Field {field} has invalid value {value}, expected one of {valid_values}", None, None))
            if not pd.isna(max_length) and expected_type.lower() in ["string", "text"]:
                long_values = gdf[field].str.len() > max_length
                if long_values.any():
                    long_indices = gdf[long_values].index.tolist()
                    for idx in long_indices:
                        issue_indices.add(idx)
                        value = gdf.loc[idx, field]
                        issue_details.append((idx, "Exceeds Max Length", f"Field {field} value {value} exceeds max length {max_length}", None, None))
        if issue_indices:
            return gdf.loc[list(issue_indices)], issue_details
        return None, issue_details

    def generate_summary_pdf(self, output_dir, layer_summary, total_layers, total_features, unmatched_layers):
        pdf = FPDF(orientation="L", unit="mm", format="A4")
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        
        # Add title
        pdf.set_font("Arial", "B", 16)
        pdf.cell(200, 10, txt="Database Quality Assurance Report", ln=True, align="C")
        pdf.ln(10)
        
        # Add database summary
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Database Summary", ln=True)
        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt=f"Total Layers: {total_layers}", ln=True)
        pdf.cell(200, 10, txt=f"Total Features: {total_features}", ln=True)
        pdf.ln(10)
        
        # Add layer-wise summary as a table
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Layer-wise Summary", ln=True)
        pdf.ln(5)
        
        # Table headers
        pdf.set_font("Arial", "B", 12)
        pdf.cell(60, 10, "Layer Name", border=1, align="C")
        pdf.cell(40, 10, "Duplicates", border=1, align="C")
        pdf.cell(40, 10, "Overlaps", border=1, align="C")
        pdf.cell(40, 10, "Line Issues", border=1, align="C")
        pdf.cell(40, 10, "Short Lines", border=1, align="C")
        pdf.cell(40, 10, "Attribute Issues", border=1, align="C")
        pdf.ln()
        
        # Table rows
        pdf.set_font("Arial", size=12)
        for layer, summary in layer_summary.items():
            pdf.cell(60, 10, layer, border=1, align="C")
            pdf.cell(40, 10, str(summary["duplicates"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["overlaps"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["line_issues"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["short_lines"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["attribute_issues"]), border=1, align="C")
            pdf.ln()
        
        # Add section for missing or unmatched layers
        pdf.ln(10)
        pdf.set_font("Arial", "B", 14)
        pdf.cell(200, 10, txt="Layers Missing or Unmatched in Dictionary", ln=True)
        pdf.set_font("Arial", size=12)
        if unmatched_layers:
            for layer in sorted(unmatched_layers):  # Sort for consistent output
                pdf.cell(200, 10, txt=f"- {layer}", ln=True)
        else:
            pdf.cell(200, 10, txt="All layers found in dictionary and matched.", ln=True)
        
        # Save the PDF
        pdf_file = os.path.join(output_dir, "database_summary_report.pdf")
        pdf.output(pdf_file)
        print(f"Summary report saved to {pdf_file}")
        
        # Update the PDF link label
        self.pdf_link_label.setText(f"<a href='file:///{pdf_file}'>Quality Assessment Report</a>")
        self.pdf_link_label.show()
        
        # Update the folder link label
        self.folder_link_label.setText(f"<a href='file:///{output_dir}'>Open Output Folder</a>")
        self.folder_link_label.show()

    def run_all_checks(self):
        try:
            selected_layers = self.get_selected_layers()
            if not selected_layers:
                QMessageBox.warning(self, "No Layers Selected", "Please select at least one layer to process.")
                return
            self.progress_bar.show()
            self.progress_label.show()
            self.progress_bar.setValue(0)
            os.makedirs(self.output_folder, exist_ok=True)
            layer_summary = {}
            total_features = 0
            unmatched_layers = []

            with fiona.Env():
                total_layers = len(selected_layers)
                self.log_message(f"Total Number of selected layers: {total_layers}")
                self.progress_bar.setRange(0, total_layers)

                for i, layer in enumerate(selected_layers):
                    self.progress_bar.setValue(i + 1)
                    self.progress_label.setText(f"Processing Layer {i + 1} of {total_layers}: {layer}")
                    QApplication.processEvents()
                    self.log_message(f"Processing Layer: {layer}")
                    gdf = gpd.read_file(self.gdb_path, layer=layer)
                    gdf = self.validate_geodataframe(gdf)
                    total_features += len(gdf)
                    gdf["feature_id"] = range(1, len(gdf) + 1)
                    gdf = self.make_timezone_naive(gdf)
                    duplicate_geoms, duplicate_pairs = self.check_duplicate_geometries(gdf)
                    duplicate_attrs = self.check_duplicate_attributes(gdf)
                    overlapping_polys, overlap_pairs = self.check_overlapping_polygons(gdf)
                    line_issues, line_issue_details = self.check_sharp_turns_self_intersections(gdf)
                    short_lines, short_line_details = self.check_short_linear_features(gdf)
                    attribute_issues, attribute_issue_details = self.check_attributes(gdf, layer, unmatched_layers)
                    layer_summary[layer] = {
                        "duplicates": len(duplicate_pairs) if duplicate_pairs else 0,
                        "overlaps": len(overlap_pairs) if overlap_pairs else 0,
                        "line_issues": len(line_issue_details) if line_issue_details else 0,
                        "short_lines": len(short_line_details) if short_line_details else 0,
                        "attribute_issues": len(attribute_issue_details) if attribute_issue_details else 0
                    }
                    if duplicate_geoms is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_duplicate_geometries.gpkg")
                        duplicate_geoms.to_file(issue_file, driver="GPKG")
                        print(f"  - Duplicate geometries saved to {issue_file}")
                        if duplicate_pairs:
                            duplicate_pairs_df = pd.DataFrame(duplicate_pairs, columns=["Feature1", "Feature2"])
                            duplicate_pairs_df[["Feature1", "Feature2"]] = np.sort(duplicate_pairs_df[["Feature1", "Feature2"]], axis=1)
                            duplicate_pairs_df = duplicate_pairs_df.drop_duplicates()
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            excel_file = os.path.join(self.output_folder, f"{layer}_duplicates.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                duplicate_pairs_df.to_excel(writer, sheet_name="Duplicate Pairs", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Unique duplicate pairs and all features saved to {excel_file}")
                    if duplicate_attrs is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_duplicate_attributes.gpkg")
                        duplicate_attrs.to_file(issue_file, driver="GPKG")
                        print(f"  - Duplicate attributes saved to {issue_file}")
                    if overlapping_polys is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_overlapping_polygons.gpkg")
                        overlapping_polys.to_file(issue_file, driver="GPKG")
                        print(f"  - Overlapping polygons saved to {issue_file}")
                        if overlap_pairs:
                            overlap_pairs_df = pd.DataFrame(overlap_pairs, columns=["Feature1", "Feature2", "Overlap Area (m²)"])
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            excel_file = os.path.join(self.output_folder, f"{layer}_overlaps.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                overlap_pairs_df.to_excel(writer, sheet_name="Overlap Pairs", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Overlapping pairs and all features saved to {excel_file}")
                    if line_issues is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_line_issues.gpkg")
                        line_issues.to_file(issue_file, driver="GPKG")
                        print(f"  - Line issues saved to {issue_file}")
                        if line_issue_details:
                            line_issue_details_df = pd.DataFrame(line_issue_details, columns=["FeatureIndex", "IssueType", "Angle", "x", "y"])
                            line_issue_details_df["feature_id"] = gdf.iloc[line_issue_details_df["FeatureIndex"]]["feature_id"].values
                            line_issue_details_df = line_issue_details_df[["feature_id", "FeatureIndex", "IssueType", "Angle", "x", "y"]]
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            excel_file = os.path.join(self.output_folder, f"{layer}_line_issues.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                line_issue_details_df.to_excel(writer, sheet_name="Line Issues", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Line issues and all features saved to {excel_file}")
                    if short_lines is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_short_lines.gpkg")
                        short_lines.to_file(issue_file, driver="GPKG")
                        print(f"  - Short linear features saved to {issue_file}")
                        if short_line_details:
                            short_line_details_df = pd.DataFrame(short_line_details, columns=["FeatureID", "Length (m)"])
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            excel_file = os.path.join(self.output_folder, f"{layer}_short_lines.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                short_line_details_df.to_excel(writer, sheet_name="Short Lines", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Short linear features and all features saved to {excel_file}")
                    if attribute_issues is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_attribute_issues.gpkg")
                        attribute_issues.to_file(issue_file, driver="GPKG")
                        print(f"  - Attribute issues saved to {issue_file}")
                        if attribute_issue_details:
                            attribute_issue_details_df = pd.DataFrame(
                                attribute_issue_details,
                                columns=["FeatureIndex", "IssueType", "Description", "x", "y"]
                            )
                            attribute_issue_details_df["feature_id"] = attribute_issue_details_df["FeatureIndex"].apply(
                                lambda x: gdf.iloc[x]["feature_id"] if x >= 0 else None
                            )
                            attribute_issue_details_df = attribute_issue_details_df[["feature_id", "FeatureIndex", "IssueType", "Description", "x", "y"]]
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            excel_file = os.path.join(self.output_folder, f"{layer}_attribute_issues.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                attribute_issue_details_df.to_excel(writer, sheet_name="Attribute Issues", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Attribute issues and all features saved to {excel_file}")
            self.generate_summary_pdf(self.output_folder, layer_summary, total_layers, total_features, unmatched_layers)
            self.progress_bar.hide()
            self.progress_label.hide()
            QMessageBox.information(self, "Run All Checks", "All checks have been completed.")
        except Exception as e:
            self.progress_label.setText(f"Error: {str(e)}")
            self.progress_bar.hide()
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    dialog = ProcessGDBDialog()
    dialog.show()
    sys.exit(app.exec_())