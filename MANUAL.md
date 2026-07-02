# Connector for ODK — User Manual

**Version 2.0**  
**Author:** Felix Mutua · [mutua@ags.co.ke](mailto:mutua@ags.co.ke)  
**Homepage:** [https://github.com/fnmutua/connector-for-ODK](https://github.com/fnmutua/connector-for-ODK)

---

## 1. Introduction

**Connector for ODK** is a QGIS plugin with four tools:


| Tool            | Purpose                                                                |
| --------------- | ---------------------------------------------------------------------- |
| **Get Data**    | Download ODK Central form submissions and load them as map layers      |
| **Split Layer** | Split a vector layer into separate layers by attribute value           |
| **QA/QC**       | Run quality checks on File Geodatabase layers and export issue reports |
| **Import**      | Upload QGIS vector features to a KeSMIS server                         |


Each dialog includes a collapsible **Help** panel. Click **« Show Help** to open it.

![QGIS menu and toolbar](screenshots/figure1.png)

*Figure 1 — QGIS menu and toolbar with Get Data, Split Layer, QA/QC, and Import*

---

## 2. Prerequisites

### 2.1 QGIS

- **QGIS 3.0 or later** (desktop)
- An internet connection for ODK Central, KeSMIS, and optional package installation

### 2.2 Python packages

The plugin installs missing packages automatically the first time it loads. Required packages:


| Package     | Used for                         |
| ----------- | -------------------------------- |
| `numpy`     | Data processing                  |
| `pandas`    | Tables and spreadsheets          |
| `geopandas` | Spatial data handling            |
| `fiona`     | Reading/writing geospatial files |
| `shapely`   | Geometry operations              |
| `fpdf`      | QA/QC PDF reports                |
| `pyproj`    | Coordinate reference systems     |


> **Tip:** QGIS ships with many geospatial libraries. If automatic installation fails, open **OSGeo4W Shell** (Windows) or your QGIS Python environment and run:
>
> ```
> python -m pip install numpy pandas geopandas fiona shapely fpdf pyproj
> ```

![Manual package installation](screenshots/figure2.png)

*Figure 2 — Manual installation of Python packages (OSGeo4W Shell or QGIS Python console)*

### 2.3 Data and access

Depending on which tools you use, you may also need:

- **Get Data** — ODK Central URL, username, and password
- **Split Layer** — Vector layers already loaded in the QGIS project
- **QA/QC** — An ESRI File Geodatabase (`.gdb` folder)
- **Import** — KeSMIS server URL, username, and password; vector layers loaded in QGIS

---

## 3. Installing the Plugin

### Option A — QGIS Plugin Repository (recommended)

1. Open **QGIS**.
2. Go to **Plugins → Manage and Install Plugins**.
3. Search for **Connector for ODK**.
4. Click **Install Plugin**.
5. Restart QGIS if prompted.

![Plugin Manager search](screenshots/figure3.png)

*Figure 3 — Plugin Manager with Connector for ODK selected*

### Option B — Install from ZIP

1. Download `connect_odk.zip` (version 2.0).
2. In QGIS, go to **Plugins → Manage and Install Plugins**.
3. Open the **Install from ZIP** tab.
4. Select the ZIP file and click **Install Plugin**.
5. Restart QGIS if prompted.

![Install from ZIP](screenshots/figure4.png)

*Figure 4 — Installing the plugin from a ZIP file*

### Verify installation

After QGIS restarts, you should see:

- Menu: **Plugins → Connector for ODK**
- Toolbar icons for **Get Data**, **Split Layer**, **QA/QC**, and **Import**

![Plugins menu](screenshots/figure5.png)

*Figure 5 — Plugins menu with all four tools listed*

---

## 4. Get Data (ODK Central)

Download ODK form submissions and add them to your map.

![Get Data login](screenshots/figrue6-getdata.png)

*Figure 6 — Get Data dialog with ODK Central login*

### Steps

1. Open **Plugins → Connector for ODK → Get Data**.
2. Enter your **ODK Central URL**, **username**, and **password**.
3. Click **Login** to load projects and forms.
4. Select a **project** and **form**.
5. Click **Process Form** to fetch submissions and add a GeoJSON layer to the map.
6. Optionally click **Get CSV** to export the data as a spreadsheet.

![Get Data ready to process](screenshots/figure7.png)

*Figure 7 — Project and form selected before processing*

![Map with submissions layer](screenshots/figure8.png)

*Figure 8 — Submissions loaded as a layer on the QGIS map*

### Notes

- Use **Save Credentials** to store your URL and login for next time.
- Output is in **EPSG:4326** (WGS 84).
- A `submissions.json` file is written to the working folder.
- Check the **Log** panel at the bottom of the dialog for progress and errors.

![Get Data help panel](screenshots/figure9.png)

*Figure 9 — Collapsible help panel in Get Data*

---

## 5. Split Layer

Create separate in-memory layers for each unique value in a chosen attribute field.

![Split Layer dialog](screenshots/figure15.png)

*Figure 15 — Split Layer dialog*

### Steps

1. Load the source vector layer in QGIS.
2. Open **Plugins → Connector for ODK → Split Layer**.
3. Choose a **Layer** from the project.
4. Choose the **Field** to split on.
5. Click **Split Layer**.

### Result

See **Figure 15** for the Split Layer dialog. After splitting:

- One new layer is created per unique non-null value.
- Layers are named `{layer}_{value}`.
- Empty fields are dropped from each split layer.
- Geometry and CRS are copied from the source.

---

## 6. QA/QC

Run quality checks on File Geodatabase layers and export issue layers, spreadsheets, and a PDF summary.

### Steps

1. Open **Plugins → Connector for ODK → QA/QC**.
2. Click **Select GeoDatabase** and choose the folder containing your `.gdb`.
3. Click **Select Output Folder** for reports and issue layers.
4. Adjust **parameters** if needed (angle and length thresholds).
5. Under **Select Layers**, tick the layers to check, or use **Select All**.
6. Click **Run All Checks**.
7. When finished, use the **PDF Report** and **Open Output Folder** links below the log.

![QA/QC interface](screenshots/figure12a.png)

*Figure 12a — QA/QC interface*

### Checks performed


| Check                | Description                                   |
| -------------------- | --------------------------------------------- |
| Duplicate geometries | Features with identical geometry              |
| Duplicate attributes | Rows with identical non-geometry fields       |
| Overlapping polygons | Polygon pairs sharing area above 0.01 m²      |
| Line issues          | Sharp turns and self-intersections            |
| Short lines          | Line features shorter than the minimum length |
| Attribute issues     | Fields validated against `dictionary.xlsx`    |


### Parameters (defaults)


| Parameter  | Default | Purpose                             |
| ---------- | ------- | ----------------------------------- |
| Min Angle  | 1°      | Lower bound for flagged turn angles |
| Max Angle  | 45°     | Upper bound for flagged turn angles |
| Min Length | 10 m    | Flag lines shorter than this        |


![QA/QC completed run](screenshots/figure12b.png)

*Figure 12b — Completed run with report and output folder links*

### Attribute dictionary

`dictionary.xlsx` is bundled with the plugin. Each sheet should match a layer name and include columns **Attribute**, **Type**, and optionally **LEN** and **Options**. You can download a copy from the help panel link inside the QA/QC dialog.

![QA/QC help panel](screenshots/figure13.png)

*Figure 13 — Help panel showing dictionary download link*

### Outputs

For each layer and issue type, `.gpkg` and `.xlsx` files are written to the output folder. A summary PDF (`database_summary_report.pdf`) is also created. Existing output files are overwritten on re-run.

![QA/QC output folder](screenshots/figure14.png)

*Figure 14 — Example QA/QC output files in the output folder*

---

## 7. Import (KeSMIS)

Upload QGIS vector layer features to a KeSMIS server with automatic field mapping.

![Import dialog](screenshots/figure10.png)

*Figure 10 — Import dialog with layer, entity, and field mapping*

### Steps

1. Load your vector layer(s) in QGIS first.
2. Open **Plugins → Connector for ODK → Import**.
3. **Server Login** — enter the KeSMIS URL, username, and password, then click **Login**.
4. **Select Layer** — choose the QGIS vector layer to export.
5. **Select Parent Entity** — choose `settlement` or `ward` for spatial matching.
6. **Select Entity** — choose the API entity/model to submit to.
7. Review the **Field Mapping** table (see Figure 10). Use the filter box to search fields and adjust API field selections if needed.
8. Click **Submit Data to KeSMIS**.

### Code column

Layers should include a `code` column for pcode matching.

1. Click **Populate Code in Selected Layers**.
2. Tick the layers to update and click **Run**.
3. A message will confirm success or report any skipped layers. Details appear in the log.

![Populate Code dialog](screenshots/figure11.png)

*Figure 11 — Layer selection dialog for Populate Code*

### Dry run

Enable **Dry Run (Test Mode)** to submit only a limited number of records before a full upload. Set the record count in the spin box next to the checkbox.

---

## 8. Tips and Troubleshooting


| Issue                                | What to try                                                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Plugin does not appear after install | Restart QGIS. Check **Plugins → Manage and Install Plugins → Installed** that Connector for ODK is enabled. |
| Package install fails                | Install dependencies manually using the QGIS Python environment (see Section 2.2).                          |
| ODK / KeSMIS login fails             | Confirm URL, username, and password. Check network access to the server.                                    |
| Import missing `code` field          | Use **Populate Code in Selected Layers** before submitting.                                                 |
| QA/QC attribute check skipped        | Ensure `dictionary.xlsx` has a sheet matching the layer name.                                               |
| No layers in a dropdown              | Load vector layers into the QGIS project first.                                                             |


For updates, bug reports, and source code:

- **Tracker:** [https://github.com/fnmutua/connector-for-ODK](https://github.com/fnmutua/connector-for-ODK)  
- **Repository:** [https://github.com/fnmutua/ODK-Connect](https://github.com/fnmutua/ODK-Connect)

---

## Screenshot checklist


| File | What to capture |
| ------------------------------ | --------------------------------------------- |
| `figure1.png` | QGIS toolbar + Plugins menu |
| `figure2.png` | OSGeo4W Shell or pip install command |
| `figure3.png` | Plugin Manager search |
| `figure4.png` | Install from ZIP tab |
| `figure5.png` | Connector for ODK submenu |
| `figrue6-getdata.png` | Get Data — login screen |
| `figure7.png` | Get Data — project/form selected |
| `figure8.png` | Map with submissions layer |
| `figure9.png` | Get Data help panel |
| `figure15.png` | Split Layer dialog |
| `figure12a.png` | QA/QC — interface |
| `figure12b.png` | QA/QC — finished |
| `figure13.png` | QA/QC help + dictionary link |
| `figure14.png` | Output folder contents |
| `figure10.png` | Import — main dialog (includes field mapping) |
| `figure11.png` | Populate Code layer picker |


---

*Connector for ODK v2.0 — Licensed under GPL-3.0*