from PyQt5.QtWidgets import QDialog, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QFormLayout, QMessageBox
from qgis.core import QgsVectorLayer, QgsProject
from qgis.gui import QgsMapCanvas  # Ensure QgsMapCanvas is imported from qgis.gui
from PyQt5.QtWidgets import QVBoxLayout, QPushButton, QHBoxLayout

import requests


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


    def __init__(self, default_url="https://collector.org", default_username="user@gmail.com", default_password="password"):
     
        """Constructor."""
        super().__init__()

        self.settings = QSettings("AGS", "ODKConnect")

        self.setWindowTitle('Connector for ODK')
        self.setFixedSize(800, 250)  # Adjusted size for map and form

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
    #     #self.url_edit.setText(default_url)  # Set default URL
    #     # self.url_edit.setText(self.settings.value("url", default_url))  # Load saved password or default value
    #     # saved_url = self.settings.value("url", default_url).strip()  # Trim any saved value

    #    # Load saved URL or use default
    #     saved_url = self.settings.value("url", default_url).strip()  # Trim any saved value
    #     self.url_edit.setText(saved_url)  # Set the trimmed saved value
        
 
        self.url_edit.setText(self.settings.value("url", default_url).strip())  # Load saved URL or use default
        self.url_edit.textChanged.connect(self.strip_spaces)  # Connect to strip spaces method


        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        #self.username_edit.setText(default_username)  # Set default username
        self.username_edit.setText(self.settings.value("username", default_username))  # Load saved username or default value

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        #self.password_edit.setText(default_password)  # Set default password
        self.password_edit.setText(self.settings.value("password", default_password))  # Load saved password or default value

        self.save_button = QPushButton("Save Credentials")
        self.save_button.setIcon(QIcon("icon.png"))  # Optional: Add an icon to the button
        self.save_button.clicked.connect(self.save_credentials)


        self.project_combobox = QComboBox()
        self.form_combobox = QComboBox()
        
        self.filter_combobox = QComboBox()
        self.login_button = QPushButton("Login")

        self.login_button.clicked.connect(self.pre_login_with_validation)

        # Process Form button
        self.process_button = QPushButton("Process Form")
        self.process_button.clicked.connect(self.pre_process_form)
        self.process_button.setEnabled(False)  # Disable until a form is selected

        # Process Form button
        self.csv_button = QPushButton("Get CSV")
        self.csv_button.clicked.connect(self.save_geojson_as_csv)
        self.csv_button.setEnabled(False)  # Disable until a form is selected

        # Create the QGIS map canvas
        self.map_canvas = QgsMapCanvas()
        self.map_canvas.setCanvasColor(Qt.white)

        # Add widgets to layout
        form_layout.addRow("ODK Central URL:", self.url_edit)
        form_layout.addRow("Username:", self.username_edit)
        form_layout.addRow("Password:", self.password_edit)
        form_layout.addRow("Project:", self.project_combobox)
        form_layout.addRow("Form:", self.form_combobox)

        # Add Login button and Process Form button
        layout.addLayout(form_layout)
       # Create a horizontal layout for the buttons
        button_layout = QHBoxLayout()

        # Add buttons to the horizontal layout
        button_layout.addWidget(self.login_button)
        button_layout.addWidget(self.save_button)
        button_layout.addWidget(self.process_button)
        button_layout.addWidget(self.csv_button)

        # Add the button layout to the main vertical layout
        layout.addLayout(button_layout)
        


        # Set up the UI components
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 0)  # Indeterminate mode (no progress shown)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setAlignment(Qt.AlignCenter)
            
        layout.addWidget(self.progress_bar)  # Correct way to add widget to layout
        # Create the QLabel for credits



       # Create a QLabel for the logo
        logo_label = QLabel()
        pixmap = QPixmap(':/plugins/connect_odk/logo.svg')  # Replace with the actual path to your logo file
        if not pixmap.isNull():  # Ensure the logo file is loaded correctly
            pixmap = pixmap.scaled(50, 50, Qt.KeepAspectRatio, Qt.SmoothTransformation)  # Scale logo
        logo_label.setPixmap(pixmap)
        logo_label.setAlignment(Qt.AlignCenter)

        credit_label = QLabel('<a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">Powered by ODK</a>')

        credit_label.setText('''
                <div style="text-align: center;">
                    <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">Powered by ODK</a>
                </div>
            ''')
        
        credit_label.setAlignment(Qt.AlignCenter)
        credit_label.setOpenExternalLinks(True)  # Allow hyperlink to open in browser

        disclaimer_label = QLabel('''
            <div style="text-align: center; font-size: 10px; color: gray;">
                <strong>Disclaimer:</strong> This plugin, is not created, endorsed, or affiliated with ODK or its developers. For official resources, visit <a href="https://getodk.org" style="color: #0078d4; text-decoration: none;">getodk.org</a>.
            </div>
        ''')
        disclaimer_label.setOpenExternalLinks(True)


        # Add the QLabel to the layout
        # layout.addWidget(logo_label)
        layout.addWidget(credit_label)
        layout.addWidget(disclaimer_label)

        self.setLayout(layout)

        # Initially hide the progress bar
        self.progress_bar.hide()

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


    def pre_process_form(self):
      """start progress bar"""
      self.progress_bar.show()
      # Use QTimer to delay the login function by 1 second (1000 milliseconds)
      QTimer.singleShot(1000, self.process_form)

    def hide_progress(self):
      """Hide progress bar"""
      self.progress_bar.hide()
 



    def process_form(self):
        """Process the form, fetch submissions, and convert them to GeoJSON."""
        
        server_url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()
        selected_project_name = self.project_combobox.currentText()
        selected_form_name = self.form_combobox.currentText()

        #Show the progress bar
         

        # Find the project ID from the list of projects
        selected_project_id = None
        for project in self.projects:
            if project['name'] == selected_project_name:
                selected_project_id = project['id']
                break

        if selected_project_id:
            # Fetch submissions for the selected form
            try:
                form_id = self.get_form_id_from_name(selected_form_name, selected_project_id)

                
                submissions = self.fetch_submissions(server_url, username, password, selected_project_id, form_id)

                with open('submissions.json', 'w') as f:
                    json.dump(submissions, f, indent=2)
                    print(f"submissions data saved to submissions.json")
                 
                # Convert submissions to GeoJSON
                geojson_data = self.convert_to_geojson(submissions,'out.json')
                
                # Add the GeoJSON data as a layer to the map
                self.add_geojson_to_map(geojson_data,selected_form_name)
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error processing form: {str(e)}")

 
    def fetch_submissions(self, server_url, username, password, project_id, form_id):
        """Fetch submissions for the selected form using OData."""
        # Construct the OData endpoint for fetching submissions
        submissions_api_url = f"{server_url}/v1/projects/{project_id}/forms/{form_id}.svc/Submissions?%24expand=*"
 
        # Set the headers to indicate that we expect JSON data
        headers = {
            'Accept': 'application/json'  # Ensure the response is in JSON format
        }

        try:
            # Send GET request to OData endpoint with authentication and headers
            response = requests.get(submissions_api_url, auth=(username, password), headers=headers)
            
            # Raise exception if the request failed (non-2xx status code)
            response.raise_for_status()

            # Parse the JSON response
            dtr = response.json()

            # Check if the response is a dictionary and contains the expected key
            if isinstance(dtr, dict):
                # For example, if the submissions are under a 'value' key
                submissions = dtr.get('value', [])
                return submissions
            else:
                raise Exception("Unexpected response format. Expected a dictionary with 'value' key.")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching submissions: {str(e)}")

   
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
        handling cases with and without nesting.
        
        :param data_array: List of dictionaries containing 'geometry' and 'properties'
        :param output_file: The output file to save the GeoJSON data
        :return: GeoJSON FeatureCollection
        """
        features = []

        for data in data_array:
            # Flatten all parent-level properties
            parent_properties = self.flatten_properties(data)
            found_geometry = self.find_geometry(data)

            # If geometry is found at the root level, create a feature
            if found_geometry:
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
                            geojson_feature = {
                                "type": "Feature",
                                "geometry": nested_geometry,
                                "properties": combined_properties
                            }
                            features.append(geojson_feature)

        # Create a GeoJSON FeatureCollection
        geojson_collection = {
            "type": "FeatureCollection",
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
 

    def add_geojson_to_map(self, geojson_data, form_name):
        """Add GeoJSON data as separate layers to the map based on geometry type."""
        
        # Remove empty properties
        geojson_data = self.remove_empty_properties(geojson_data)
        self.geo_data = geojson_data

        # Split features by geometry type
        geometry_types = {
            "Point": [],
            "Linear": [],
            "Polygon": [],
            # Add additional geometry types if needed
        }

        # Separate features by geometry type
        for feature in geojson_data.get("features", []):
            geometry_type = feature["geometry"]["type"]
            if geometry_type == "LineString":
                geometry_types["Linear"].append(feature)
            elif geometry_type in geometry_types:
                geometry_types[geometry_type].append(feature)
        
        # Create layers for each geometry type
        for geom_type, features in geometry_types.items():
            if not features:
                continue  # Skip if no features for this geometry type
            
            # Create a GeoJSON string for this geometry type
            geom_geojson_data = {
                "type": "FeatureCollection",
                "features": features
            }
            geom_geojson_str = json.dumps(geom_geojson_data)
            
            # Create a layer for this geometry type
            vector_layer = QgsVectorLayer(geom_geojson_str, f"{form_name}_{geom_type}", "ogr")
            
            # Add the vector layer to the current map project
            QgsProject.instance().addMapLayer(vector_layer)
        
        # Optionally zoom to the extent of all added layers
        self.map_canvas.zoomToFullExtent()
        self.map_canvas.refresh()
        self.hide_progress()

        print("GeoJSON data has been added to the map with separate layers for each geometry type.")

 
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
        """Save the entered credentials."""
        # Get the entered values from the text fields
        url = self.url_edit.text()
        username = self.username_edit.text()
        password = self.password_edit.text()

        # Save them to QSettings
        self.settings.setValue("url", url)
        self.settings.setValue("username", username)
        self.settings.setValue("password", password)

        # Show a confirmation message
        QMessageBox.information(self, "Success", "Credentials saved successfully!")

        # Optionally, print or log the saved values for debugging (do not do this for passwords in production)
        print(f"Saved URL: {url}, Username: {username}")
