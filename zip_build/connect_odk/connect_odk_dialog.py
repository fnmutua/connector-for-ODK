from PyQt5.QtWidgets import QDialog, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QFormLayout, QMessageBox,QApplication
from qgis.core import QgsVectorLayer, QgsProject
from qgis.gui import QgsMapCanvas  # Ensure QgsMapCanvas is imported from qgis.gui
from PyQt5.QtWidgets import QVBoxLayout, QPushButton, QHBoxLayout
from qgis.core import QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem
import requests

from PyQt5.QtWidgets import (QDialog, QProgressBar, QVBoxLayout, QPushButton, QLabel, QCheckBox, 
                             QLineEdit, QSpinBox, QFileDialog, QComboBox, QHBoxLayout, QMessageBox, 
                             QGroupBox, QTextEdit, QScrollArea, QGridLayout, QWidget, QTableWidget, 
                             QTableWidgetItem, QSizePolicy)
import json 
from PyQt5.QtCore import Qt  # Add this import for Qt
from qgis.core import QgsVectorLayer, QgsProject
from PyQt5.QtCore import QTimer

#------- Showing dialog
from PyQt5.QtWidgets import QDialog, QProgressBar, QVBoxLayout, QMessageBox, QPushButton
from PyQt5.QtCore import Qt

import csv
import json
from qgis.PyQt.QtWidgets import QFileDialog, QMessageBox
from PyQt5.QtCore import QSettings  
from PyQt5.QtGui import QIcon

 
from qgis.core import QgsMessageLog
from collections import OrderedDict
from PyQt5.QtCore import QRegularExpression
from PyQt5.QtGui import QRegularExpressionValidator

from PyQt5.QtWidgets import QLabel
from PyQt5.QtGui import QPixmap


from PyQt5.QtWidgets import QDialog, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QFormLayout, QMessageBox, QTextEdit, QProgressBar, QLabel
from qgis.core import QgsVectorLayer, QgsProject, QgsCoordinateReferenceSystem
from qgis.gui import QgsMapCanvas
from PyQt5.QtCore import Qt, QTimer, QSettings
from PyQt5.QtGui import QPixmap, QIcon
import requests
import json
import csv
from qgis.PyQt.QtWidgets import QFileDialog
from collections import OrderedDict

from datetime import datetime  # Ensure this is in imports
from PyQt5.QtCore import QThread, pyqtSignal, QObject

import tempfile
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional imports for advanced functionality
try:
    import geopandas as gpd
    import pandas as pd
    import shortuuid
    from shapely.geometry import mapping, shape
    from fuzzywuzzy import fuzz
    from rapidfuzz import process, fuzz
except ImportError:
    # These are optional and only needed for certain features
    pass

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

class SubmissionWorker(QObject):
    """Worker to fetch submissions in a background thread."""
    progress = pyqtSignal(int)  # Emit progress percentage
    log = pyqtSignal(str)  # Emit log messages
    finished = pyqtSignal()  # Signal when done
    result = pyqtSignal(list)  # Emit fetched submissions
    error = pyqtSignal(str)  # Emit error message

    def __init__(self, server_url, username, password, project_id, form_id):
        super().__init__()
        self.server_url = server_url
        self.username = username
        self.password = password
        self.project_id = project_id
        self.form_id = form_id
        self._is_running = True

    def stop(self):
        """Signal the worker to stop execution."""
        self._is_running = False

    def run(self):
        """Fetch submissions in the background."""
        try:
            if not self._is_running:
                self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Worker stopped before starting.")
                self.result.emit([])
                self.finished.emit()
                return

            headers = {'Accept': 'application/json'}
            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Initiating submission fetch...")
            self.progress.emit(0)

            submissions_api_url = (
                f"{self.server_url}/v1/projects/{self.project_id}/forms/{self.form_id}.svc/Submissions"
                f"?%24expand=*"
            )
            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Requesting: {submissions_api_url}")
            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Please wait...")

            response = requests.get(submissions_api_url, auth=(self.username, self.password), headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                raise Exception("Unexpected response format. Expected a dictionary.")

            submissions = data.get('value', [])
            total_count = len(submissions)
            self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Retrieved {total_count} submissions.")

            if total_count == 0:
                self.log.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No submissions found.")
            self.progress.emit(100)

            self.result.emit(submissions)
            self.finished.emit()

        except requests.exceptions.RequestException as e:
            self.error.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error fetching submissions: {str(e)}")
            self.finished.emit()
        except Exception as e:
            self.error.emit(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Unexpected error: {str(e)}")
            self.finished.emit()




class ConnectODKDialog(QDialog):
    """Dialog to get user input for ODK Central credentials and form selection."""
 

    # Add a validation method
    def validate_url(self):
        url = self.url_edit.text().strip()  # Remove leading/trailing spaces
        if not url.startswith("http://") and not url.startswith("https://"):
            QMessageBox.warning(self, "Invalid URL", "Please enter a valid URL (must start with http:// or https://).")
            return False
        return True
    

    def pre_login_with_validation(self):
        if not self.validate_url():
            return  # Exit if the URL is invalid
        self.pre_login()  # Proceed with the original login logic

    def strip_spaces(self):
        """Strip leading and trailing spaces on typing."""
        current_text = self.sender().text().strip()  # Get the text and strip spaces
        current_text = current_text.rstrip('/')  # Remove any trailing slashes
        self.sender().setText(current_text)  # Set the stripped text back
 
    """Dialog to get user input for ODK Central credentials and form selection."""

    def __init__(self, default_url="https://collector.org", default_username="user@gmail.com", default_password="password"):
        """Constructor."""
        super().__init__()

        self.settings = QSettings("AGS", "ODKConnect")

        self.setWindowTitle('Connector for ODK')
        self.setFixedSize(600, 450)  # Increased height for clear button

        # Initialize variables
        self.projects = []
        self.forms = []
        self.geo_data = []

        # Create layout
        layout = QVBoxLayout()
        form_layout = QFormLayout()

        # Create widgets
        self.url_edit = QLineEdit()
        self.url_edit.setPlaceholderText("ODK Central URL")
        self.url_edit.setText(self.settings.value("url", default_url).strip())
        self.url_edit.textChanged.connect(self.strip_spaces)

        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        self.username_edit.setText(self.settings.value("username", default_username))

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.password_edit.setText(self.settings.value("password", default_password))

        self.save_button = QPushButton("Save Credentials")
        self.save_button.clicked.connect(self.save_credentials)

        self.project_combobox = QComboBox()
        self.form_combobox = QComboBox()
        self.filter_combobox = QComboBox()
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.pre_login_with_validation)

        self.process_button = QPushButton("Process Form")
        self.process_button.clicked.connect(self.pre_process_form)
        self.process_button.setEnabled(False)

        self.csv_button = QPushButton("Get CSV")
        self.csv_button.clicked.connect(self.save_geojson_as_csv)
        self.csv_button.setEnabled(False)

        # Create the QGIS map canvas
        self.map_canvas = QgsMapCanvas()
        self.map_canvas.setCanvasColor(Qt.white)

        # Add checkbox for permanent file saving
        self.save_permanent_check = QCheckBox("Save files permanently (not temporary)")
        self.save_permanent_check.setToolTip("If checked, files will be saved to a permanent location instead of temporary files")
        self.save_permanent_check.setChecked(self.settings.value("save_permanent", True, type=bool))

        # Add widgets to form layout
        form_layout.addRow("ODK Central URL:", self.url_edit)
        form_layout.addRow("Username:", self.username_edit)
        form_layout.addRow("Password:", self.password_edit)
        form_layout.addRow("Project:", self.project_combobox)
        form_layout.addRow("Form:", self.form_combobox)
        form_layout.addRow("", self.save_permanent_check)

        # Create button layout
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.login_button)
        button_layout.addWidget(self.save_button)
        button_layout.addWidget(self.process_button)
        button_layout.addWidget(self.csv_button)

        # Add progress bar
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setAlignment(Qt.AlignCenter)
        self.progress_bar.hide()

        # Add log window
        self.log_textedit = QTextEdit()
        self.log_textedit.setReadOnly(True)
        self.log_textedit.setFixedHeight(100)
        self.log_textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        # Add clear log button
        self.clear_log_button = QPushButton("Clear Log")
        self.clear_log_button.clicked.connect(self.clear_log)

        # Add cleanup button for temporary files
        self.cleanup_button = QPushButton("Clean Up Temporary Files")
        self.cleanup_button.clicked.connect(self.manual_cleanup_temp_files)
        self.cleanup_button.setToolTip("Manually clean up any temporary files created by this session")

        # Add logo and credits
        logo_label = QLabel()
        pixmap = QPixmap(':/plugins/connect_odk/logo.svg')
        if not pixmap.isNull():
            pixmap = pixmap.scaled(50, 50, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        logo_label.setPixmap(pixmap)
        logo_label.setAlignment(Qt.AlignCenter)

        credit_label = QLabel('''
            <div style="text-align: center;">
                <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">Powered by ODK</a>
            </div>
        ''')
        credit_label.setAlignment(Qt.AlignCenter)
        credit_label.setOpenExternalLinks(True)

        disclaimer_label = QLabel('''
            <div style="text-align: center; font-size: 10px; color: gray;">
                <strong>Disclaimer:</strong> This plugin is not created, endorsed, or affiliated with ODK or its developers. 
                For official resources, visit <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">getodk.org</a>.
            </div>
        ''')
        disclaimer_label.setOpenExternalLinks(True)

        # Assemble layout
        layout.addLayout(form_layout)
        layout.addLayout(button_layout)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.log_textedit)
        layout.addWidget(self.clear_log_button)
        layout.addWidget(self.cleanup_button)
        layout.addWidget(credit_label)
        layout.addWidget(disclaimer_label)

        self.setLayout(layout)
        self.submission_thread = QThread()
        self.submission_worker = None

    def log_message(self, message):
        """Append a message to the log textedit widget."""
        self.log_textedit.append(message)
        self.log_textedit.ensureCursorVisible()

    def clear_log(self):
        """Clear all messages in the log window."""
        self.log_textedit.clear()

    def pre_process_form(self):
        """Start submission fetching in a background thread with immediate UI feedback."""
        self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Starting form processing...")
        self.progress_bar.setRange(0, 0)  # Indeterminate mode
        self.progress_bar.show()
        QApplication.processEvents()  # Force UI update

        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        selected_project_name = self.project_combobox.currentText()
        selected_form_name = self.form_combobox.currentText()

        selected_project_id = None
        for project in self.projects:
            if project['name'] == selected_project_name:
                selected_project_id = project['id']
                break

        if not selected_project_id:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No project selected.")
            self.progress_bar.hide()
            return

        try:
            form_id = self.get_form_id_from_name(selected_form_name, selected_project_id)
        except Exception as e:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error: {str(e)}")
            self.progress_bar.hide()
            return

        # Create and start submission worker
        self.submission_worker = SubmissionWorker(server_url, username, password, selected_project_id, form_id)
        self.submission_worker.moveToThread(self.submission_thread)
        self.submission_worker.progress.connect(self.update_progress)
        self.submission_worker.log.connect(self.log_message)
        self.submission_worker.result.connect(self.on_submissions_fetched)
        self.submission_worker.error.connect(self.on_submission_error)
        self.submission_worker.finished.connect(self.on_submission_finished)
        self.submission_thread.started.connect(self.submission_worker.run)
        self.submission_thread.start()

    def update_progress(self, value):
        """Update progress bar value."""
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def on_submissions_fetched(self, submissions):
        """Handle fetched submissions and continue processing."""
        try:
            if not submissions:
                self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] No submissions found.")
                QMessageBox.warning(self, "No Submissions", "No submissions found for the selected form.")
                return

            with open('submissions.json', 'w') as f:
                json.dump(submissions, f, indent=2)
                self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Submissions saved to submissions.json")

            geojson_data = self.convert_to_geojson(submissions, 'out.json')
            self.add_geojson_to_map(geojson_data, self.form_combobox.currentText())

        except Exception as e:
            self.log_message(f"[{datetime.now().strftime('%H:%M:%S.%f')}] Error processing submissions: {str(e)}")
            QMessageBox.critical(self, "Error", f"Error processing form: {str(e)}")

    def on_submission_error(self, error_message):
        """Handle errors from submission worker."""
        self.log_message(error_message)
        QMessageBox.critical(self, "Error", error_message.split("] ")[-1])

    def on_submission_finished(self):
        """Clean up after submission worker finishes."""
        self.progress_bar.hide()
        self.submission_thread.quit()
        self.submission_thread.wait()
        self.submission_worker = None

    def closeEvent(self, event):
        """Handle dialog close event to clean up threads and temporary files."""
        # Clean up temporary files
        if hasattr(self, 'temp_files'):
            for temp_file in self.temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.unlink(temp_file)
                        self.log_message(f"Cleaned up temporary file: {temp_file}")
                except Exception as e:
                    self.log_message(f"Error cleaning up temporary file {temp_file}: {str(e)}")
        
        # Clean up threads
        if self.submission_thread.isRunning():
            if self.submission_worker:
                self.submission_worker.stop()
                self.submission_worker.deleteLater()
            self.submission_thread.quit()
            self.submission_thread.wait()
        super().closeEvent(event)


 

    def get_form_data(self):
        """Return the form data entered by the user."""
        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        selected_project = self.project_combobox.currentText()
        selected_form = self.form_combobox.currentText()
        return server_url, username, password, selected_project, selected_form

    def set_projects_and_forms(self, projects, forms=[]):
        """Set the available projects and forms in the comboboxes."""
        self.project_combobox.clear()
        self.project_combobox.addItems([project['name'] for project in projects])
        
        # Clear and disable form combobox until a project is selected
        self.form_combobox.clear()
        self.form_combobox.setEnabled(True)

    def pre_login(self):
        """start progress bar"""
        self.progress_bar.show()
        # Use QTimer to delay the login function by 1 second (1000 milliseconds)
        QTimer.singleShot(1000, self.login)


    def login(self):
        """Login to ODK Central and fetch projects and forms."""
        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
      
        # Fetch projects
        try:
            
            projects = self.fetch_projects(server_url, username, password)
            self.projects = projects  # Store the fetched projects
            # Initially hide the progress bar
            self.progress_bar.hide()

            # Populate the project combobox
            self.set_projects_and_forms(projects)

            # Enable the project combobox and disable the form combobox until a project is selected
            self.project_combobox.setEnabled(True)
            self.form_combobox.setEnabled(True)

            # Automatically select the first project (index 0)
            self.project_combobox.setCurrentIndex(0)

            # Trigger the on_project_selected method manually after setting the index
            
            self.on_project_selected()

            # Connect the signal when a project is selected to fetch forms
            self.project_combobox.currentIndexChanged.connect(self.on_project_selected)

        except Exception as e:
            # Display error message to the user
            error_message = f"Error fetching projects: {str(e)}"
            QMessageBox.critical(self, "Login Error", error_message)
            self.progress_bar.hide()

            # Optionally, you can also raise the exception if you want to propagate it further
            #raise

 
    def on_project_selected(self):
        """Fetch forms when a project is selected."""
        selected_project_name = self.project_combobox.currentText()

        
        # Find the project ID from the list of projects
        selected_project_id = None
        for project in self.projects:
            if project['name'] == selected_project_name:
                selected_project_id = project['id']
                break

        if selected_project_id:
            try:
                # Fetch forms for the selected project
 
         
                forms = self.fetch_forms(self.url_edit.text(), self.username_edit.text(), self.password_edit.text(), selected_project_id)
                 
                # Store the forms in self.forms
                self.forms = forms  # Store the fetched forms

                # Populate the form combobox
                self.form_combobox.clear()
                self.form_combobox.addItems([form['name'] for form in forms])
                self.form_combobox.setEnabled(True)

                # Enable Process Form button after form selection
                self.process_button.setEnabled(True)

            except Exception as e:
                print(f"Error fetching forms: {str(e)}")
                self.form_combobox.setEnabled(False)

    def fetch_projects(self, server_url, username, password):
        """Fetch projects from ODK Central."""
        
        projects_api_url = f"{server_url}/v1/projects"
        
        try:
            response = requests.get(projects_api_url, auth=(username, password))
            response.raise_for_status()
            projects = response.json()
            #self.progress_bar.hide()
            return projects
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching projects: {str(e)}")

    def fetch_forms(self, server_url, username, password, project_id):
        """Fetch forms for the selected project."""
        forms_api_url = f"{server_url}/v1/projects/{project_id}/forms"
        try:
            response = requests.get(forms_api_url, auth=(username, password))
            response.raise_for_status()
            forms = response.json()
            return forms
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching forms: {str(e)}")

    def get_form_id_from_name(self, form_name, project_id):
        """Helper function to get the form ID based on the form name."""

        
        if not self.forms:
            raise Exception("No forms available. Please select a project.")
        
        for form in self.forms:
            if form['name'] == form_name:
                return form['xmlFormId']
        
        raise Exception(f"Form ID not found for form: {form_name}")

 
    def hide_progress(self):
      """Hide progress bar"""
      self.progress_bar.hide()
 
 

    def find_geometry(self, data):
        """
        Recursively search for GeoJSON geometry in the data.
        :param data: Dictionary that might contain GeoJSON geometry
        :return: The GeoJSON geometry (or None if not found)
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    if 'type' in value and 'coordinates' in value:
                        return value
                    geometry = self.find_geometry(value)
                    if geometry:
                        return geometry
        elif isinstance(data, list):
            for item in data:
                geometry = self.find_geometry(item)
                if geometry:
                    return geometry
        return None

    def flatten_properties(self, d):
        """
        Flatten a nested dictionary to extract leaf nodes only.
        :param d: Dictionary to flatten
        :return: Flattened dictionary
        """
        leaves = {}
        for key, value in d.items():
            if isinstance(value, dict):
                leaves.update(self.flatten_properties(value))
            elif not isinstance(value, list):  # Skip lists
                leaves[key] = value
        return leaves

    def convert_to_geojson(self, data_array, output_file):
        """
        Convert a list of data dictionaries into a GeoJSON FeatureCollection,
        handling cases with and without nesting, with 5 decimal precision and EPSG:4326 CRS.
        
        :param data_array: List of dictionaries containing 'geometry' and 'properties'
        :param output_file: The output file to save the GeoJSON data
        :return: GeoJSON FeatureCollection
        """
        features = []

        def round_coordinates(geometry):
            """Recursively round coordinates to 5 decimal places."""
            if isinstance(geometry, dict) and 'coordinates' in geometry:
                if isinstance(geometry['coordinates'], list):
                    geometry['coordinates'] = [
                        [
                            round(c, 5) if isinstance(c, (int, float)) else c
                            for c in coords
                        ] if isinstance(coords, list) else round(coords, 5)
                        for coords in geometry['coordinates']
                    ]
            return geometry

        for data in data_array:
            # Flatten all parent-level properties
            parent_properties = self.flatten_properties(data)
            found_geometry = self.find_geometry(data)

            # If geometry is found at the root level, create a feature
            if found_geometry:
                found_geometry = round_coordinates(found_geometry)
                geojson_feature = {
                    "type": "Feature",
                    "geometry": found_geometry,
                    "properties": parent_properties
                }
                features.append(geojson_feature)
                continue

            # If no root-level geometry, look for nested data structures
            for key, value in data.items():
                if isinstance(value, list):
                    for item in value:
                        # Flatten each nested structure and find geometry
                        nested_geometry = self.find_geometry(item)
                        nested_properties = self.flatten_properties(item)

                        # Combine parent properties with nested properties
                        combined_properties = {**parent_properties, **nested_properties}

                        if nested_geometry:
                            nested_geometry = round_coordinates(nested_geometry)
                            geojson_feature = {
                                "type": "Feature",
                                "geometry": nested_geometry,
                                "properties": combined_properties
                            }
                            features.append(geojson_feature)

        # Create a GeoJSON FeatureCollection with CRS
        geojson_collection = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:EPSG::4326"
                }
            },
            "features": features
        }

        # Save GeoJSON data to the specified file
        with open(output_file, 'w') as f:
            json.dump(geojson_collection, f, indent=2)

        print(f"GeoJSON data saved to {output_file}")
        self.csv_button.setEnabled(True)

        return geojson_collection
    
    
        
    def remove_empty_properties(self,geojson_data):
        """Remove empty properties from GeoJSON features."""
        for feature in geojson_data.get('features', []):
            # Filter out empty properties for each feature
            feature['properties'] = {key: value for key, value in feature['properties'].items() if value not in [None, '', [], {}, {}, False]}
        return geojson_data
 

 
    def cleanup_temp_files(self):
        """Clean up any existing temporary files."""
        if hasattr(self, 'temp_files'):
            for temp_file in self.temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.unlink(temp_file)
                        self.log_message(f"Cleaned up old temporary file: {temp_file}")
                except Exception as e:
                    self.log_message(f"Error cleaning up old temporary file {temp_file}: {str(e)}")
            self.temp_files = []

    def add_geojson_to_map(self, geojson_data, form_name):
        """Add GeoJSON data as separate layers to the map based on geometry type.
        Saves data to temporary files first to prevent QGIS hanging with virtual layers."""
        
        # Clean up any existing temporary files first
        self.cleanup_temp_files()
        
        # Remove empty properties
        geojson_data = self.remove_empty_properties(geojson_data)
        self.geo_data = geojson_data

        # Split features by geometry type
        geometry_types = {
            "Point": [],
            "Linear": [],
            "Polygon": [],
        }

        # Separate features by geometry type
        for feature in geojson_data.get("features", []):
            geometry_type = feature["geometry"]["type"]
            if geometry_type in ["LineString", "MultiLineString"]:
                geometry_types["Linear"].append(feature)
            elif geometry_type in ["Point", "MultiPoint"]:
                geometry_types["Point"].append(feature)
            elif geometry_type in ["Polygon", "MultiPolygon"]:
                geometry_types["Polygon"].append(feature)
            else:
                # For any other geometry types, add to Linear as fallback
                geometry_types["Linear"].append(feature)
        
        # Create layers for each geometry type
        for geom_type, features in geometry_types.items():
            if not features:
                continue  # Skip if no features for this geometry type
            
            # Create a GeoJSON string for this geometry type
            geom_geojson_data = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:EPSG::4326"
                    }
                },
                "features": features
            }
            
            # Save to temporary file instead of creating virtual layer
            try:
                # Determine file path based on user preference
                if self.save_permanent_check.isChecked():
                    # Ask user for save location
                    file_path, _ = QFileDialog.getSaveFileName(
                        self, 
                        f"Save {geom_type} Layer", 
                        f"{form_name}_{geom_type}.geojson", 
                        "GeoJSON Files (*.geojson);;All Files (*)"
                    )
                    if not file_path:
                        self.log_message(f"Skipping {geom_type} layer - no file path selected")
                        continue
                    if not file_path.endswith('.geojson'):
                        file_path += '.geojson'
                    
                    # Write GeoJSON data to permanent file
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(geom_geojson_data, f, indent=2)
                    
                    self.log_message(f"Saved {geom_type} layer to permanent file: {file_path}")
                else:
                    # Create temporary file
                    temp_file = tempfile.NamedTemporaryFile(
                        mode='w', 
                        suffix='.geojson', 
                        delete=False,
                        encoding='utf-8'
                    )
                    file_path = temp_file.name
                    
                    # Write GeoJSON data to temporary file
                    json.dump(geom_geojson_data, temp_file, indent=2)
                    temp_file.close()
                    
                    # Store the temporary file path for cleanup later
                    if not hasattr(self, 'temp_files'):
                        self.temp_files = []
                    self.temp_files.append(file_path)
                
                # Create layer from file
                layer_name = f"{form_name}_{geom_type}"
                vector_layer = QgsVectorLayer(file_path, layer_name, "ogr")
                
                if vector_layer.isValid():
                    # Set CRS explicitly
                    vector_layer.setCrs(QgsCoordinateReferenceSystem("EPSG:4326"))
                    
                    # Add the vector layer to the current map project
                    QgsProject.instance().addMapLayer(vector_layer)
                    
                    self.log_message(f"Successfully loaded {len(features)} {geom_type} features from file")
                else:
                    self.log_message(f"Failed to load {geom_type} layer from file")
                    # Clean up the file if layer creation failed (only for temp files)
                    if not self.save_permanent_check.isChecked():
                        try:
                            os.unlink(file_path)
                        except:
                            pass
                        
            except Exception as e:
                self.log_message(f"Error creating {geom_type} layer: {str(e)}")
                # Clean up the file if there was an error (only for temp files)
                if not self.save_permanent_check.isChecked() and 'file_path' in locals():
                    try:
                        os.unlink(file_path)
                    except:
                        pass
        
        # Optionally zoom to the extent of all added layers
        self.map_canvas.zoomToFullExtent()
        self.map_canvas.refresh()
        self.hide_progress()

        if self.save_permanent_check.isChecked():
            self.log_message("GeoJSON data has been added to the map with separate layers for each geometry type.")
            self.log_message("Layers are loaded from permanent files to prevent QGIS hanging.")
        else:
            self.log_message("GeoJSON data has been added to the map with separate layers for each geometry type.")
            self.log_message("Layers are loaded from temporary files to prevent QGIS hanging.")
            self.log_message("Temporary files will be cleaned up when the dialog is closed.")

    def extract_headers_from_geojson(self,features):
        """
        Extract all unique property keys from GeoJSON features in the order they are encountered.

        :param features: List of GeoJSON features.
        :return: List of unique headers including geometry fields.
        """
        headers = OrderedDict()  # Use OrderedDict to preserve order
        for feature in features:
            if isinstance(feature, dict) and "properties" in feature:
                for key in feature["properties"].keys():
                    headers[key] = None  # Add keys in order of their first appearance

        # Add geometry fields to the headers
        return list(headers.keys()) + ["latitude", "longitude"]

    def save_geojson_as_csv(self):
        """
        Save GeoJSON data as a CSV file.

        :param self: Reference to the plugin instance.
        """
        try:
            # Ensure GeoJSON data is a dictionary
            geo = self.geo_data
            QgsMessageLog.logMessage("Starting the process to save GeoJSON as CSV...", "GeoJSON to CSV")

            if isinstance(geo, str):
                try:
                    geo = json.loads(geo)
                except json.JSONDecodeError:
                    QMessageBox.warning(self, "Error", "Invalid GeoJSON string.")
                    return
            elif not isinstance(geo, dict):
                raise ValueError("GeoJSON data must be a dictionary or a valid JSON string.")

            if "features" not in geo:
                QMessageBox.warning(self, "Error", "GeoJSON data is missing 'features' key.")
                return

            features = geo.get("features", [])
            if not features:
                QMessageBox.warning(self, "Error", "No features found in GeoJSON.")
                return

            # Extract headers in order
            headers = self.extract_headers_from_geojson(features)

            # Prompt user for file location
            output_file, _ = QFileDialog.getSaveFileName(
                self, "Save CSV File", "", "CSV Files (*.csv);;All Files (*)"
            )
            if not output_file:
                QMessageBox.warning(self, "Cancelled", "No file selected.")
                return
            if not output_file.endswith(".csv"):
                output_file += ".csv"

            # Write to CSV
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()

                for feature in features:
                    if isinstance(feature, dict):
                        row = feature.get("properties", {}).copy()

                        # Add geometry fields for point geometries
                        geometry = feature.get("geometry", {})
                        if geometry and geometry.get("type", "") == "Point":
                            coordinates = geometry.get("coordinates", [])
                            if len(coordinates) >= 2:
                                row["latitude"] = coordinates[1]
                                row["longitude"] = coordinates[0]
                            else:
                                row["latitude"] = None
                                row["longitude"] = None
                        else:
                            row["latitude"] = None
                            row["longitude"] = None

                        writer.writerow(row)

            QgsMessageLog.logMessage(f"CSV successfully saved to {output_file}", "GeoJSON to CSV")
            QMessageBox.information(self, "Success", f"CSV saved to {output_file}")

        except Exception as e:
            QgsMessageLog.logMessage(f"Error occurred: {str(e)}", "GeoJSON to CSV")
            QMessageBox.critical(self, "Error", f"An error occurred: {str(e)}")

    def save_credentials(self):
        """Save the entered credentials and preferences."""
        # Get the entered values from the text fields
        url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        save_permanent = self.save_permanent_check.isChecked()

        # Save them to QSettings
        self.settings.setValue("url", url)
        self.settings.setValue("username", username)
        self.settings.setValue("password", password)
        self.settings.setValue("save_permanent", save_permanent)

        # Show a confirmation message
        QMessageBox.information(self, "Success", "Credentials and preferences saved successfully!")

        # Optionally, print or log the saved values for debugging (do not do this for passwords in production)
        print(f"Saved URL: {url}, Username: {username}, Save Permanent: {save_permanent}")

    def manual_cleanup_temp_files(self):
        """Manually clean up temporary files."""
        self.cleanup_temp_files()
        self.log_message("Temporary files have been cleaned up.")
        QMessageBox.information(self, "Cleanup Complete", "Temporary files have been cleaned up successfully.")
