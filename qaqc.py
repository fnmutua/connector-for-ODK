
import subprocess
import sys

# List of required packages
required_packages = [
    "geopandas", "fiona", "numpy", "pandas", "shapely", "fpdf","pyproj",
]

# Function to check and install missing packages----------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
def install_missing_packages(packages):
    for package in packages:
        try:
            __import__(package)  # Try importing the package
        except ImportError:
            print(f"{package} not found. Installing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

            

from PyQt5.QtWidgets import QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, QGroupBox,QTextEdit
from qgis.core import (
    QgsProject, 
    QgsVectorLayer,
    QgsFeatureRequest,
    QgsFields,
    QgsFeature,
    QgsWkbTypes,
)

from PyQt5.QtCore import Qt  # Add this import for Qt
from PyQt5.QtWidgets import QApplication  # Add this import

import geopandas as gpd
import fiona
import os
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Polygon, MultiPolygon, MultiLineString,Point
from shapely.strtree import STRtree
from fpdf import FPDF  # For generating PDF reports
import pyproj
from shapely.ops import transform



class ProcessGDBDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Quality Assurance / Quality Control")
        self.setFixedSize(1000, 600)  # Increased height to accommodate new section

        # Layout and widgets for the dialog
        layout = QVBoxLayout()

        # Label
        self.label = QLabel("Select a GeoDatabase, output folder, and set criteria")
        
        # Button to select GeoDatabase
        self.gdb_button = QPushButton("Select GeoDatabase")
        self.gdb_button.clicked.connect(self.select_gdb)
        
        # Label to display selected GDB
        self.gdb_label = QLabel("No GeoDatabase selected")
        self.gdb_label.setStyleSheet("font-style: italic; color: gray;")
        
        # Button to select output folder
        self.output_button = QPushButton("Select Output Folder")
        self.output_button.setEnabled(False)  # Disable until a database is selected
        self.output_button.clicked.connect(self.select_output_folder)
        
        # Label to display selected output folder
        self.output_label = QLabel("No output folder selected")
        self.output_label.setStyleSheet("font-style: italic; color: gray;")
        
        # Parameters Section
        parameters_box = QGroupBox("Set Parameters")
        parameters_layout = QVBoxLayout()
        


            # Angular Parameters Group
        params_group = QGroupBox("Linear Feature Parameters")
        angular_params_layout = QVBoxLayout()

        # Spin boxes for min and max angle
        self.min_angle_spinbox = QSpinBox()
        self.min_angle_spinbox.setRange(0, 360)
        self.min_angle_spinbox.setPrefix("Min Angle: ")
        self.min_angle_spinbox.setValue(20)  # Default value for max angle

        self.max_angle_spinbox = QSpinBox()
        self.max_angle_spinbox.setRange(0, 360)
        self.max_angle_spinbox.setPrefix("Max Angle: ")
        self.max_angle_spinbox.setValue(45)  # Default value for max angle

        # Add min/max angle to road parameters group
        angular_params_layout.addWidget(self.min_angle_spinbox)
        angular_params_layout.addWidget(self.max_angle_spinbox)
        params_group.setLayout(angular_params_layout)

        # Add road parameters group to the main parameters layout
        parameters_layout.addWidget(params_group)


  
        # length Parameters Group
        length_group = QGroupBox("Length Parameters")
        length_params_layout = QVBoxLayout()

        # Spin boxes for min and max angle
        self.min_length_spinbox = QSpinBox()
        self.min_length_spinbox.setRange(0, 50)
        self.min_length_spinbox.setPrefix("Min Length(m): ")
        self.min_length_spinbox.setValue(10)  # Default value for max angle
 
        # Add min/max angle to road parameters group
        length_params_layout.addWidget(self.min_length_spinbox) 
        length_group.setLayout(length_params_layout)

        # Add road parameters group to the main parameters layout
        parameters_layout.addWidget(length_group)
        





        # Add more parameter widgets here if needed
        # Example: self.parameter_spinbox = QSpinBox()
        # parameters_layout.addWidget(self.parameter_spinbox)
        
        parameters_box.setLayout(parameters_layout)
        
        # Processing Buttons Layout
        button_layout = QHBoxLayout()
        button_box = QGroupBox("Processing Options")
        button_box.setLayout(button_layout)
        
        # self.duplicates_button = QPushButton("Check Duplicates")
        # self.duplicates_button.clicked.connect(self.check_duplicates)
        
        # self.short_lines_button = QPushButton("Check Short Lines")
        # self.short_lines_button.clicked.connect(self.check_short_lines)
        
        # self.user_input_button = QPushButton("Custom Check")
        # self.user_input_button.clicked.connect(self.custom_check)
        
        # self.overlapping_polygons_button = QPushButton("Check Overlapping Polygons")
        # self.overlapping_polygons_button.clicked.connect(self.check_overlapping_polygons)
        
        # self.sharp_turns_button = QPushButton("Check Sharp Turns & Intersections")
        # self.sharp_turns_button.clicked.connect(self.check_sharp_turns)
        
        self.run_all_button = QPushButton("Run All Checks")
        self.run_all_button.setEnabled(False)  # Disable until a output folder is specificed  
        self.run_all_button.clicked.connect(self.run_all_checks)
    
            # Add a QTextEdit for logging
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)  # Make it read-only
        

        # button_layout.addWidget(self.duplicates_button)
        # button_layout.addWidget(self.short_lines_button)
        # button_layout.addWidget(self.user_input_button)
        # button_layout.addWidget(self.overlapping_polygons_button)
        # button_layout.addWidget(self.sharp_turns_button)
        button_layout.addWidget(self.run_all_button)
        
        # Add widgets to layout
        layout.addWidget(self.label)
        layout.addWidget(self.gdb_button)
        layout.addWidget(self.gdb_label)
        layout.addWidget(self.output_button)
        layout.addWidget(self.output_label)
        layout.addWidget(parameters_box)
        layout.addWidget(button_box)
        layout.addWidget(self.log_textedit)  # Add it to the layout



        # Set up the UI components
         # Add a QLabel for progress status
        self.progress_label = QLabel("Progress: Idle")
        layout.addWidget(self.progress_label)

        # Set up the progress bar
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 100)  # Set initial range (0-100%)
        self.progress_bar.setValue(0)  # Start at 0%
        self.progress_bar.setTextVisible(True)  # Show percentage text
        layout.addWidget(self.progress_bar)

        # Hide the progress bar initially
        self.progress_bar.hide()
        self.progress_label.hide()


        # Inside the __init__ method of ProcessGDBDialog
        self.pdf_link_label = QLabel("PDF Report: <a href='#'>Open Report</a>")
        self.pdf_link_label.setOpenExternalLinks(True)  # Allow opening external links
        self.pdf_link_label.setStyleSheet("color: blue; text-decoration: underline;")
        self.pdf_link_label.hide()  # Hide initially
        layout.addWidget(self.pdf_link_label)  # Add it to the layout


        # Inside the __init__ method of ProcessGDBDialog
        self.folder_link_label = QLabel("Open Folder: <a href='#'>Open Output Folder</a>")
        self.folder_link_label.setOpenExternalLinks(True)  # Allow opening external links
        self.folder_link_label.setStyleSheet("color: blue; text-decoration: underline;")
        self.folder_link_label.hide()  # Hide initially
        layout.addWidget(self.folder_link_label)  # Add it to the layout


        self.setLayout(layout)
        

    def log_message(self, message):
        """Append a message to the log widget."""
        self.log_textedit.append(message)

    def select_gdb(self):
        """Open a folder dialog to select a GeoDatabase (which is a folder)."""
        gdb_path = QFileDialog.getExistingDirectory(self, "Select GeoDatabase Folder")
        if gdb_path:
            self.gdb_label.setText(gdb_path)
            self.gdb_path=gdb_path
            self.output_button.setEnabled(True)

    def select_output_folder(self):
        """Open a file dialog to select an output folder."""
        output_folder = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if output_folder:
            self.output_label.setText(output_folder)
            self.output_folder=output_folder
            self.run_all_button.setEnabled(True)  # Disable until a output folder is specificed 

    def check_duplicates(self):
        """Check for duplicates in the GeoDatabase."""
        QMessageBox.information(self, "Check Duplicates", "Checking for duplicate geometries or attributes...")
    
    def check_short_lines(self):
        """Check for short lines in linear features."""
        QMessageBox.information(self, "Check Short Lines", "Checking for short line features...")
    
    def custom_check(self):
        """Allow user input for a custom check."""
        QMessageBox.information(self, "Custom Check", "Performing user-defined checks...")
    
    def check_overlapping_polygons(self):
        """Check for overlapping polygons."""
        QMessageBox.information(self, "Check Overlapping Polygons", "Checking for overlapping polygons...")
    
    def check_sharp_turns(self):
        """Check for sharp turns and self-intersections."""
        QMessageBox.information(self, "Check Sharp Turns", "Checking for sharp turns and self-intersections...")
    
    
    def validate_geodataframe(self,gdf):
        """Ensure the GeoDataFrame has only one geometry column."""
        if "geometry" in gdf.columns and gdf.geometry.name != "geometry":
            gdf = gdf.set_geometry("geometry", inplace=False)
        return gdf

    def make_timezone_naive(self,gdf):
        """Convert timezone-aware datetime columns to timezone-naive."""
        for col in gdf.columns:
            if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                gdf[col] = gdf[col].dt.tz_localize(None)  # Remove timezone
        return gdf
    
    def check_duplicate_geometries(self,gdf):
        """
        Identify duplicate geometries in a GeoDataFrame.
        Skips None geometries and returns a DataFrame of duplicates with their pairs.
        """
        duplicate_pairs = []
        seen = {}

        for idx, geom in gdf.geometry.items():
            if geom is None or geom.is_empty:  # Skip None or empty geometries
                print(f"Skipping None or empty geometry at index {idx}")
                continue

            geom_wkt = geom.wkt  # Use WKT for exact comparison
            if geom_wkt in seen:
                duplicate_pairs.append((seen[geom_wkt], idx))
            else:
                seen[geom_wkt] = idx

        if duplicate_pairs:
            duplicate_indices = {idx for pair in duplicate_pairs for idx in pair}  # Set of indices
            return gdf.loc[list(duplicate_indices)], duplicate_pairs  # Convert set to list

        return None, None

    def check_duplicate_attributes(self,gdf):
        """Identify features with identical attribute values excluding geometry."""
        attr_columns = [col for col in gdf.columns if col != "geometry"]
        duplicates = gdf[gdf.duplicated(subset=attr_columns, keep=False)]
        exact_duplicates = duplicates[duplicates.duplicated(subset=attr_columns, keep="first")]
        return exact_duplicates if not exact_duplicates.empty else None

    def check_overlapping_polygons(self,gdf, tolerance=0.01):
        """Find polygon features that truly overlap, calculate overlap area, and return overlapping pairs."""
        
        # Validate and reproject the GeoDataFrame
        if gdf.crs != "EPSG:21037":
            print("Reprojecting to EPSG:21037 (Arc 1960 / UTM zone 37N) for accurate area calculation...")
            gdf = gdf.to_crs(epsg=21037)  # Ensure area calculations are in meters
        
        tree = STRtree(gdf.geometry)
        overlap_pairs = []
        
        for idx, geom in gdf.geometry.items():
            if isinstance(geom, (Polygon, MultiPolygon)):
                possible_matches = [i for i in tree.query(geom) if i != idx]
                
                for idx2 in possible_matches:
                    geom2 = gdf.geometry.iloc[idx2]
                    
                    # Ensure it is a polygon and check if they intersect
                    if isinstance(geom2, (Polygon, MultiPolygon)) and geom.intersects(geom2):
                        
                        # Compute the intersection
                        intersection = geom.intersection(geom2)
                        
                        # Ensure the intersection is not just a line or point
                        if not intersection.is_empty and intersection.area > tolerance:
                            overlap_area = intersection.area  # Area in square meters
                            overlap_pairs.append((idx, idx2, overlap_area))  # Store indices and overlap area
        
        if overlap_pairs:
            overlap_indices = set([idx for pair in overlap_pairs for idx in [pair[0], pair[1]]])
            return gdf.iloc[list(overlap_indices)], overlap_pairs
        
        return None, None


    def check_sharp_turns_self_intersections(self,gdf, lower_angle_threshold=15, upper_angle_threshold=30):
        """Find sharp turns and self-intersections in line features.
        
        Args:
            gdf: GeoDataFrame containing line geometries.
            lower_angle_threshold: Minimum acceptable angle (in degrees).
            upper_angle_threshold: Maximum acceptable angle (in degrees).
        
        Returns:
            A tuple containing:
            - A GeoDataFrame with features that have issues.
            - A list of tuples with details about the issues.
        """
        gdf = self.validate_geodataframe(gdf)
        issue_indices = set()
        issue_details = []

        lower_angle_threshold=self.min_angle_spinbox.value()
        upper_angle_threshold=self.max_angle_spinbox.value()

        # Define the projection transformers
        wgs84 = pyproj.CRS('EPSG:4326')  # WGS84 coordinate system
        original_crs = pyproj.CRS(gdf.crs)  # Original CRS of the GeoDataFrame
        transformer = pyproj.Transformer.from_crs(original_crs, wgs84, always_xy=True)

        for idx, geom in enumerate(gdf.geometry):
            if geom is None:
                continue  # Skip empty geometries
            
            # Convert MultiLineString into individual LineStrings
            lines = [geom] if isinstance(geom, LineString) else list(geom.geoms) if isinstance(geom, MultiLineString) else []
            
            for line in lines:
                if len(line.coords) < 3:
                    continue  # Skip lines that are too short for angle calculation
                
                coords = np.array(line.coords)

                # Check for sharp turns
                for i in range(1, len(coords) - 1):
                    v1 = coords[i] - coords[i - 1]
                    v2 = coords[i + 1] - coords[i]

                    norm_v1 = np.linalg.norm(v1)
                    norm_v2 = np.linalg.norm(v2)

                    if norm_v1 > 0 and norm_v2 > 0:
                        dot_product = np.dot(v1, v2)
                        angle = np.degrees(np.arccos(dot_product / (norm_v1 * norm_v2)))
                        
                        # Check if the angle is outside the acceptable range
                        if lower_angle_threshold <= angle <= upper_angle_threshold:
                            issue_indices.add(idx)
                            # Transform the coordinates to WGS84
                            print(angle)
                            lon, lat = transformer.transform(coords[i][0], coords[i][1])
                            issue_details.append((idx, "Angle Out of Range", round(angle, 2), lat, lon))

                # Check for self-intersections
                if not line.is_simple:
                    issue_indices.add(idx)
                    issue_details.append((idx, "Self-Intersection", None))

        if issue_indices:
            return gdf.iloc[list(issue_indices)], issue_details
        return None, None
    def check_short_linear_features(self,gdf ):
        """
        Identify linear features shorter than the specified length threshold.
        
        Args:
            gdf (GeoDataFrame): The input GeoDataFrame.
            length_threshold (float): The minimum length threshold in meters (default: 10).
        
        Returns:
            GeoDataFrame: A GeoDataFrame containing features shorter than the threshold.
            list: A list of tuples containing (feature_id, length) for short features.
        """
        gdf = self.validate_geodataframe(gdf)
        length_threshold=self.min_length_spinbox.value()
        
        # Reproject to EPSG:21037 (Arc 1960 / UTM zone 37N) for accurate length calculation
        if gdf.crs != "EPSG:21037":
            print("Reprojecting to EPSG:21037 (Arc 1960 / UTM zone 37N) for accurate length calculation...")
            gdf = gdf.to_crs(epsg=21037)  # Ensure length calculations are in meters
        
        short_features = []

        for idx, geom in enumerate(gdf.geometry):
            #print(gdf.geom_type)
            if isinstance(geom, (LineString,MultiLineString)):
                length = geom.length  # Length in meters
                #print(length)
                if length < length_threshold:
                    short_features.append((gdf.iloc[idx]["feature_id"], length))
        
        if short_features:
            short_indices = [idx for idx, _ in short_features]
            return gdf.iloc[short_indices], short_features
        return None, None

    def generate_summary_pdf(self, output_dir, layer_summary, total_layers, total_features):
        """Generate a PDF summary report for the database with a table format."""
        pdf =  FPDF(orientation="L", unit="mm", format="A4")
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

        pdf.ln()
        
        # Table rows
        pdf.set_font("Arial", size=12)
        for layer, summary in layer_summary.items():
            pdf.cell(60, 10, layer, border=1, align="C")
            pdf.cell(40, 10, str(summary["duplicates"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["overlaps"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["line_issues"]), border=1, align="C")
            pdf.cell(40, 10, str(summary["short_lines"]), border=1, align="C")

            pdf.ln()
        
        # Save the PDF
        pdf_file = os.path.join(output_dir, "database_summary_report.pdf")
        pdf.output(pdf_file)
        print(f"Summary report saved to {pdf_file}")
         # Update the PDF link label
        self.pdf_link_label.setText(f"<a href='file:///{pdf_file}'>Quality Assessment Report</a>")
        self.pdf_link_label.show()  # Show the link
        

        # Update the folder link label
        self.folder_link_label.setText(f"<a href='file:///{output_dir}'>Open Output Folder</a>")
        self.folder_link_label.show()  # Show the folder link
        
    
    def xrun_all_checks(self):
        """Run all checks sequentially."""
        """Process all layers in the geodatabase and save rows with issues and detailed Excel reports."""
        os.makedirs(self.output_folder, exist_ok=True)
        layer_summary = {}
        total_features = 0
        self.progress_bar.show()

        with fiona.Env():
            layers = fiona.listlayers(self.gdb_path)
            total_layers = len(layers)
            self.log_message(f"Total Number of layers: {total_layers}")  # Log to QTextEdit

            for layer in layers:
                #self.progress_bar.show() 
                self.log_message(f"Processing Layer: {layer}")  # Log to QTextEdit

                gdf = gpd.read_file(self.gdb_path, layer=layer)
                gdf = self.validate_geodataframe(gdf)
                total_features += len(gdf)
                
                # Add a unique feature_id to each feature
                gdf["feature_id"] = range(1, len(gdf) + 1)
                
                # Convert timezone-aware datetime columns to timezone-naive
                gdf = self.make_timezone_naive(gdf)
                duplicate_geoms, duplicate_pairs = self.check_duplicate_geometries(gdf)
                duplicate_attrs = self.check_duplicate_attributes(gdf)
                overlapping_polys, overlap_pairs = self.check_overlapping_polygons(gdf)
                line_issues, line_issue_details = self.check_sharp_turns_self_intersections(gdf)
                short_lines, short_line_details = self.check_short_linear_features(gdf,self.min_length_spinbox)



                # Summarize issues for the layer
                layer_summary[layer] = {
                    "duplicates": len(duplicate_pairs) if duplicate_pairs else 0,
                    "overlaps": len(overlap_pairs) if overlap_pairs else 0,
                    "line_issues": len(line_issue_details) if line_issue_details else 0,
                    "short_lines": len(short_line_details) if short_line_details else 0,

                }
                
                # Save rows with issues
                if duplicate_geoms is not None:
                    issue_file = os.path.join(self.output_folder, f"{layer}_duplicate_geometries.gpkg")
                    duplicate_geoms.to_file(issue_file, driver="GPKG")
                    print(f"  - Duplicate geometries saved to {issue_file}")
                    
                    # Save detailed Excel for duplicate pairs and all features
                if duplicate_pairs:
                    # Convert duplicate pairs to DataFrame
                    duplicate_pairs_df = pd.DataFrame(duplicate_pairs, columns=["Feature1", "Feature2"])
                    
                    # Ensure unique duplicate pairs by sorting Feature1 and Feature2 in each row
                    duplicate_pairs_df[["Feature1", "Feature2"]] = np.sort(duplicate_pairs_df[["Feature1", "Feature2"]], axis=1)
                    
                    # Drop duplicate rows to remove reversed duplicates
                    duplicate_pairs_df = duplicate_pairs_df.drop_duplicates()

                    # Select all features excluding geometry
                    all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]

                    # Define output Excel file path
                    excel_file = os.path.join(self.output_folder, f"{layer}_duplicates.xlsx")
                    
                    # Write data to Excel
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
                        # Create a DataFrame for overlapping pairs with overlap area
                        overlap_pairs_df = pd.DataFrame(overlap_pairs, columns=["Feature1", "Feature2", "Overlap Area (m²)"])
                        
                        # Create a DataFrame for all features with feature_id
                        all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                        
                        # Save to Excel
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
                        # Convert issue details to DataFrame
                        line_issue_details_df = pd.DataFrame(line_issue_details, columns=["FeatureIndex", "IssueType", "Angle","Lat","Lon"])

                        # Merge feature_id from gdf using index
                        line_issue_details_df["feature_id"] = gdf.iloc[line_issue_details_df["FeatureIndex"]]["feature_id"].values

                        # Reorder columns to place feature_id first
                        line_issue_details_df = line_issue_details_df[["feature_id", "FeatureIndex", "IssueType", "Angle","Lat","Lon"]]

                        # Create DataFrame for all features excluding geometry
                        all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]

                        # Save to Excel
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
                        # Create a DataFrame for short linear features
                        short_line_details_df = pd.DataFrame(short_line_details, columns=["FeatureID", "Length (m)"])
                        
                        # Create a DataFrame for all features with feature_id
                        all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                        
                        # Save to Excel
                        excel_file = os.path.join(self.output_folder, f"{layer}_short_lines.xlsx")
                        with pd.ExcelWriter(excel_file) as writer:
                            short_line_details_df.to_excel(writer, sheet_name="Short Lines", index=False)
                            all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                        print(f"  - Short linear features and all features saved to {excel_file}")


            self.generate_summary_pdf(self.output_folder, layer_summary, total_layers, total_features)
            self.progress_bar.hide()

 
        QMessageBox.information(self, "Run All Checks", "All checks have been completed.") 




    def run_all_checks(self):
        """Run all checks sequentially."""
        try:
            # Initialize progress bar and label
            self.progress_bar.show()
            self.progress_label.show()
            self.progress_bar.setValue(0)  # Reset progress bar

            os.makedirs(self.output_folder, exist_ok=True)
            layer_summary = {}
            total_features = 0

            with fiona.Env():
                layers = fiona.listlayers(self.gdb_path)
                total_layers = len(layers)
                self.log_message(f"Total Number of layers: {total_layers}")

                # Set progress bar range based on total layers
                self.progress_bar.setRange(0, total_layers)

                for i, layer in enumerate(layers):
                    # Update progress bar and label
                    self.progress_bar.setValue(i + 1)
                    self.progress_label.setText(f"Processing Layer {i + 1} of {total_layers}: {layer}")
                    QApplication.processEvents()  # Keep the UI responsive

                    self.log_message(f"Processing Layer: {layer}")
                    gdf = gpd.read_file(self.gdb_path, layer=layer)
                    gdf = self.validate_geodataframe(gdf)
                    total_features += len(gdf)

                    # Add a unique feature_id to each feature
                    gdf["feature_id"] = range(1, len(gdf) + 1)

                    # Convert timezone-aware datetime columns to timezone-naive
                    gdf = self.make_timezone_naive(gdf)

                    # Perform checks
                    duplicate_geoms, duplicate_pairs = self.check_duplicate_geometries(gdf)
                    duplicate_attrs = self.check_duplicate_attributes(gdf)
                    overlapping_polys, overlap_pairs = self.check_overlapping_polygons(gdf)
                    line_issues, line_issue_details = self.check_sharp_turns_self_intersections(gdf)
                    short_lines, short_line_details = self.check_short_linear_features(gdf)

                    # Summarize issues for the layer
                    layer_summary[layer] = {
                        "duplicates": len(duplicate_pairs) if duplicate_pairs else 0,
                        "overlaps": len(overlap_pairs) if overlap_pairs else 0,
                        "line_issues": len(line_issue_details) if line_issue_details else 0,
                        "short_lines": len(short_line_details) if short_line_details else 0,
                    }

                    # Save rows with issues (existing code)
                    # ...
                    # Save rows with issues
                    if duplicate_geoms is not None:
                        issue_file = os.path.join(self.output_folder, f"{layer}_duplicate_geometries.gpkg")
                        duplicate_geoms.to_file(issue_file, driver="GPKG")
                        print(f"  - Duplicate geometries saved to {issue_file}")
                        
                        # Save detailed Excel for duplicate pairs and all features
                    if duplicate_pairs:
                        # Convert duplicate pairs to DataFrame
                        duplicate_pairs_df = pd.DataFrame(duplicate_pairs, columns=["Feature1", "Feature2"])
                        
                        # Ensure unique duplicate pairs by sorting Feature1 and Feature2 in each row
                        duplicate_pairs_df[["Feature1", "Feature2"]] = np.sort(duplicate_pairs_df[["Feature1", "Feature2"]], axis=1)
                        
                        # Drop duplicate rows to remove reversed duplicates
                        duplicate_pairs_df = duplicate_pairs_df.drop_duplicates()

                        # Select all features excluding geometry
                        all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]

                        # Define output Excel file path
                        excel_file = os.path.join(self.output_folder, f"{layer}_duplicates.xlsx")
                        
                        # Write data to Excel
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
                            # Create a DataFrame for overlapping pairs with overlap area
                            overlap_pairs_df = pd.DataFrame(overlap_pairs, columns=["Feature1", "Feature2", "Overlap Area (m²)"])
                            
                            # Create a DataFrame for all features with feature_id
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            
                            # Save to Excel
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
                            # Convert issue details to DataFrame
                            line_issue_details_df = pd.DataFrame(line_issue_details, columns=["FeatureIndex", "IssueType", "Angle","Lat","Lon"])

                            # Merge feature_id from gdf using index
                            line_issue_details_df["feature_id"] = gdf.iloc[line_issue_details_df["FeatureIndex"]]["feature_id"].values

                            # Reorder columns to place feature_id first
                            line_issue_details_df = line_issue_details_df[["feature_id", "FeatureIndex", "IssueType", "Angle","Lat","Lon"]]

                            # Create DataFrame for all features excluding geometry
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]

                            # Save to Excel
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
                            # Create a DataFrame for short linear features
                            short_line_details_df = pd.DataFrame(short_line_details, columns=["FeatureID", "Length (m)"])
                            
                            # Create a DataFrame for all features with feature_id
                            all_features_df = gdf[["feature_id"] + [col for col in gdf.columns if col != "geometry"]]
                            
                            # Save to Excel
                            excel_file = os.path.join(self.output_folder, f"{layer}_short_lines.xlsx")
                            with pd.ExcelWriter(excel_file) as writer:
                                short_line_details_df.to_excel(writer, sheet_name="Short Lines", index=False)
                                all_features_df.to_excel(writer, sheet_name="All Features", index=False)
                            print(f"  - Short linear features and all features saved to {excel_file}")

            # Generate summary PDF
            self.generate_summary_pdf(self.output_folder, layer_summary, total_layers, total_features)

            # Hide progress bar and label after completion
            self.progress_bar.hide()
            self.progress_label.hide()

            QMessageBox.information(self, "Run All Checks", "All checks have been completed.")
        except Exception as e:
            # Handle errors and update progress bar
            self.progress_label.setText(f"Error: {str(e)}")
            self.progress_bar.hide()
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")