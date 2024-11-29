from qgis.PyQt.QtCore import QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction
from .connect_odk_dialog import ConnectODKDialog
import requests
import os
from .split_layer_dialog import SplitLayerDialog  # Import the new SplitLayerDialog


class ConnectODK:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor."""
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.dlg = None
        self.first_start = True
        self.actions = []  # Initialize the actions list

    def tr(self, message):
        """Get the translation for a string using Qt translation API."""
        return QCoreApplication.translate('ConnectODK', message)

    def add_action(self, icon_path, text, callback, enabled_flag=True, add_to_menu=True, add_to_toolbar=True, status_tip=None, whats_this=None, parent=None):
        """Add a toolbar icon to the toolbar."""
        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(self.tr(u'&ODK Connect'), action)
            

        self.actions.append(action)  # Add action to self.actions list
        return action


    def open_split_layer_dialog(self):
            """Open the Split Layer dialog."""
            dialog = SplitLayerDialog()  # Create the SplitLayerDialog
            dialog.exec_()  # Open the dialog
            
    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        
        # First, check if actions already exist and remove them if they do
        if hasattr(self, 'menu_actions'):
            # Remove existing actions from the menu
            for action in self.menu_actions:
                self.iface.mainWindow().menuBar().removeAction(action)
        
        # Now, add the new actions
        icon_path = ':/plugins/plugin_reloader/reload.png'
        #icon_path = ':/plugins/connect_odk/icon.png'
        get_data_action = self.add_action(icon_path, text=self.tr(u'Get Data'), callback=self.run, parent=self.iface.mainWindow())
        

        icon_path = ':/plugins/Generalizer3/icon.png'
        split_layer_action = self.add_action(icon_path, text=self.tr(u'Split Layer'), callback=self.open_split_layer_dialog, parent=self.iface.mainWindow())

        # Store the actions so they can be removed when reloading
        self.menu_actions = [get_data_action, split_layer_action]


    def unload(self):
        """Remove the plugin menu item and icon from QGIS GUI."""
        for action in self.actions:  # Now this should work
            self.iface.removePluginMenu(self.tr(u'&Connect ODK'), action)
            self.iface.removeToolBarIcon(action) 

        # First, check if actions already exist and remove them if they do
        if hasattr(self, 'menu_actions'):
            # Remove existing actions from the menu
            for action in self.menu_actions:
                self.iface.mainWindow().menuBar().removeAction(action)
        


    def run(self):
        """Run method that performs all the real work."""
        if self.first_start:
            self.first_start = False
            self.dlg = ConnectODKDialog()  # Create the main dialog

        # Get the form data from the main dialog
        result = self.dlg.exec_()

        if result:
            server_url, username, password, selected_project, selected_form = self.dlg.get_form_data()

            # Fetch projects
            try:
                projects = self.fetch_projects(server_url, username, password)
                self.dlg.projects = projects  # Store the fetched projects
                self.dlg.project_combobox.setEnabled(True)
                self.dlg.form_combobox.setEnabled(False)

            except Exception as e:
                print(f"Error: {str(e)}")
                self.reject()
                return

            # After projects are fetched, allow user to select project and form
            self.dlg.set_projects_and_forms(projects, [])
            self.dlg.project_combobox.currentIndexChanged.connect(self.on_project_selected)

    def on_project_selected(self):
        """Handle when a project is selected."""
        selected_project = self.dlg.project_combobox.currentText()

        # Fetch forms for the selected project
        selected_project_id = None
        for project in self.dlg.projects:
            if project['name'] == selected_project:
                selected_project_id = project['id']
                break

        if selected_project_id:
            try:
                forms = self.fetch_forms(self.dlg.url_edit.text(), self.dlg.username_edit.text(), self.dlg.password_edit.text(), selected_project_id)
                self.dlg.form_combobox.setEnabled(True)
                self.dlg.form_combobox.clear()
                self.dlg.form_combobox.addItems([form['name'] for form in forms])
            except Exception as e:
                print(f"Error: {str(e)}")
                self.reject()

    def fetch_projects(self, server_url, username, password):
        """Fetch projects from ODK Central."""
        projects_api_url = f"{server_url}/v1/projects"
        try:
            response = requests.get(projects_api_url, auth=(username, password))
            response.raise_for_status()
            projects = response.json()

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
