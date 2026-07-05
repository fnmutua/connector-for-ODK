# Connector for ODK — User Manual

**Version 2 — Standard** (Get Data, Split Layer, and QA/QC)  
**Plugin version 2.0**  
**Author:** Felix Mutua · [mutua@ags.co.ke](mailto:mutua@ags.co.ke)  
**Homepage:** [https://github.com/fnmutua/connector-for-ODK](https://github.com/fnmutua/connector-for-ODK)

---

## 1. Introduction

**Connector for ODK** is a QGIS plugin with three tools:


| Tool            | Purpose                                                                |
| --------------- | ---------------------------------------------------------------------- |
| **Get Data**    | Download ODK Central form submissions and load them as map layers      |
| **Split Layer** | Split a vector layer into separate layers by attribute value           |
| **QA/QC**       | Run quality checks on File Geodatabase layers and export issue reports |


Each dialog includes a collapsible **Help** panel. The QA/QC dialog opens with help visible; other tools show **« Show Help** to open it.

![QGIS menu and toolbar](screenshots/figure1.png)

*Figure 1 — QGIS menu and toolbar with Get Data, Split Layer, and QA/QC*

---

## 2. Prerequisites

### 2.1 QGIS

- **QGIS 3.0 or later** (desktop)
- An internet connection for ODK Central and optional package installation

### 2.2 Python packages

The plugin installs missing packages automatically the first time it loads. If that fails, install dependencies manually using **OSGeo4W Shell** from your QGIS installation (Windows).

**Open OSGeo4W Shell**

1. Close QGIS.
2. Open **OSGeo4W Shell** from your QGIS installation folder — for example `C:\Program Files\QGIS 3.x\bin\OSGeo4W.bat` — or from the Windows Start menu under your QGIS install (e.g. **QGIS Desktop → OSGeo4W Shell**).

> Use the OSGeo4W Shell that ships with the same QGIS version you use. Do not use a separate OSGeo4W install, or packages may install into the wrong Python environment.

**Install required packages**

Run this command in that shell:

```
python -m pip install numpy pandas geopandas fiona shapely pyproj fpdf2 requests fuzzywuzzy rapidfuzz shortuuid openpyxl xlsxwriter
```

Required packages:


| Package      | Used for                           |
| ------------ | ---------------------------------- |
| `numpy`      | Numerical processing               |
| `pandas`     | Tables and spreadsheets            |
| `geopandas`  | Spatial data handling              |
| `fiona`      | Reading/writing geospatial files   |
| `shapely`    | Geometry operations                |
| `pyproj`     | Coordinate reference systems       |
| `fpdf2`      | QA/QC PDF reports (`import fpdf`)  |
| `requests`   | ODK Central API calls              |
| `fuzzywuzzy` | Fuzzy field and attribute matching |
| `rapidfuzz`  | Fast field matching (plugin load)  |
| `shortuuid`  | Unique ID generation (plugin load) |
| `openpyxl`   | Reading `dictionary.xlsx` (QA/QC)  |
| `xlsxwriter` | Writing QA/QC Excel outputs        |


![Manual package installation in OSGeo4W Shell](screenshots/figure2.png)

*Figure 2 — Installing Python packages in OSGeo4W Shell from your QGIS installation folder*

**Troubleshooting: fixing a missing package error**

If QGIS shows a yellow bar such as *"Couldn't load plugin 'connect_odk'…"* or the **Message Log** reports `ModuleNotFoundError: No module named '…'`, a required Python package is not installed in the QGIS Python environment.

![Missing package error banner](screenshots/figure-2b-stacktrace.png)

*Figure 3 — Plugin load error when a required Python package is missing*

Click **View message log** (or open **View → Panels → Log Messages → Python Error**) to see which package is missing.

![Message log with ModuleNotFoundError](screenshots/figure-2c-missingpackage.png)

*Figure 4 — Message log showing the missing package (example: `shortuuid`)*

**How to fix it**

1. Note the **package name** in the error (the name inside quotes after `No module named`).
2. **Close QGIS** completely.
3. Open **OSGeo4W Shell** from your QGIS installation folder (see above).
4. Install the missing package. You can install **all** required packages (recommended):

```
python -m pip install numpy pandas geopandas fiona shapely pyproj fpdf2 requests fuzzywuzzy rapidfuzz shortuuid openpyxl xlsxwriter
```

   Or install **only the missing package** (replace `shortuuid` with the name from your error):

```
python -m pip install shortuuid
```

5. **Restart QGIS**. Connector for ODK should load without the error.

### 2.3 Data and access

Depending on which tools you use, you may also need:

- **Get Data** — ODK Central URL, username, and password
- **Split Layer** — Vector layers already loaded in the QGIS project
- **QA/QC** — An ESRI File Geodatabase (`.gdb` folder)

---

## 3. Installing the Plugin

### Option A — QGIS Plugin Repository (recommended)

1. Open **QGIS**.
2. Go to **Plugins → Manage and Install Plugins**.
3. Search for **Connector for ODK**.
4. Click **Install Plugin**.
5. Restart QGIS if prompted.

![Plugin Manager search](screenshots/figure3.png)

*Figure 5 — Plugin Manager with Connector for ODK selected*

### Option B — Install from ZIP

1. Download `connect_odk.zip` (version 2.0).
2. In QGIS, go to **Plugins → Manage and Install Plugins**.
3. Open the **Install from ZIP** tab.
4. Select the ZIP file and click **Install Plugin**.
5. Restart QGIS if prompted.

![Install from ZIP](screenshots/figure4.png)

*Figure 6 — Installing the plugin from a ZIP file*

### Verify installation

After QGIS restarts, you should see:

- Menu: **Plugins → Connector for ODK**
- Toolbar icons for **Get Data**, **Split Layer**, and **QA/QC**

![Plugins menu](screenshots/figure5.png)

*Figure 7 — Plugins menu with Get Data, Split Layer, and QA/QC*

---

## 4. Get Data (ODK Central)

Download ODK form submissions and add them to your map.

![Get Data login](screenshots/figrue6-getdata.png)

*Figure 8 — Get Data dialog with ODK Central login*

### Steps

1. Open **Plugins → Connector for ODK → Get Data**.
2. Enter your **ODK Central URL**, **username**, and **password**.
3. Click **Login** to load projects and forms.
4. Select a **project** and **form**.
5. Click **Process Form** to fetch submissions and add a GeoJSON layer to the map.
6. Optionally click **Get CSV** to export the data as a spreadsheet.

![Get Data ready to process](screenshots/figure7.png)

*Figure 9 — Project and form selected before processing*

![Map with submissions layer](screenshots/figure8.png)

*Figure 10 — Submissions loaded as a layer on the QGIS map*

### Notes

- Use **Save Credentials** to store your URL and login for next time.
- Output is in **EPSG:4326** (WGS 84).
- A `submissions.json` file is written to the working folder.
- Check the **Log** panel at the bottom of the dialog for progress and errors.

![Get Data help panel](screenshots/figure9.png)

*Figure 11 — Collapsible help panel in Get Data*

---

## 5. Split Layer

Create separate in-memory layers for each unique value in a chosen attribute field.

### Steps

1. Load the source vector layer in QGIS.
2. Open **Plugins → Connector for ODK → Split Layer**.
3. Choose a **Layer** from the project.
4. Choose the **Field** to split on.
5. Click **Split Layer**.

![Split Layer dialog](screenshots/figure15.png)

*Figure 12 — Split Layer dialog*

### Result

After splitting:

- One new layer is created per unique non-null value.
- Layers are named `{layer}_{value}`.
- Empty fields are dropped from each split layer.
- Geometry and CRS are copied from the source.

---

## 6. QA/QC

Run quality checks on File Geodatabase layers and export issue layers, spreadsheets, and a PDF summary.

### Steps

1. Open **Plugins → Connector for ODK → QA/QC**. The dialog opens with the help panel visible.
2. From **Quick start** step 2 in the help panel, download the **template geodatabase** and **dictionary.xlsx** for reference when aligning your data (see below).
3. Click **Select GeoDatabase** and choose the folder containing your `.gdb`. Use the template to structure new submissions.
4. Click **Select Output Folder** for reports and issue layers.
5. Adjust **parameters** if needed (angle and length thresholds).
6. Under **Select Layers**, tick the layers to check, or use **Select All**. The layer list scrolls inside a fixed panel so **Run All Checks** stays visible.
7. Click **Run All Checks**.
8. When finished, use the **PDF Report** and **Open Output Folder** links below the log.

![QA/QC interface](screenshots/figure12a.png)

*Figure 13 — QA/QC interface*

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

*Figure 14 — Completed run with report and output folder links*

### Template geodatabase and attribute dictionary

The template geodatabase and attribute dictionary are **reference materials** for packaging and QA/QC. They describe the expected layer names, fields, types, and geometry so your `.gdb` aligns with the real submission structure. Download copies from **Quick start** step 2 in the QA/QC help panel.

1. **Download template geodatabase** — shows the expected layer layout. Use it as a guide when building or checking your geodatabase.
2. **Download dictionary.xlsx** — lists the field names, types, and geometry for each layer. Open the **How to** sheet for column definitions; every other sheet describes one layer.
3. **Using both together** — consultants use the template and dictionary as reference when packaging data so layers and attributes match what QA/QC expects.
4. **Help panel downloads** — links open a save dialog without clearing the help text.

![QA/QC help panel](screenshots/figure13.png)

*Figure 15 — Help panel showing template geodatabase and dictionary download links*

### Outputs

For each layer and issue type, `.gpkg` and `.xlsx` files are written to the output folder. A summary PDF (`database_summary_report.pdf`) is also created. Existing output files are overwritten on re-run.

![QA/QC output folder](screenshots/figure14.png)

*Figure 16 — Example QA/QC output files in the output folder*

---

## 7. Tips and Troubleshooting


| Issue                                | What to try                                                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Plugin does not appear after install | Restart QGIS. Check **Plugins → Manage and Install Plugins → Installed** that Connector for ODK is enabled. |
| Package install fails                | See **Fixing a missing package error** in Section 2.2. Close QGIS, open **OSGeo4W Shell** from your QGIS installation folder, install the missing package (or all packages), then restart QGIS. |
| ODK login fails                      | Confirm URL, username, and password. Check network access to ODK Central.                                   |
| QA/QC attribute check skipped        | Ensure `dictionary.xlsx` has a sheet matching the layer name. Open the **How to** sheet in the dictionary for layer and field definitions. |
| Help panel text disappears after a download link | Reload the plugin if this happens on an older build; current versions keep the help text visible. |
| No layers in a dropdown              | Load vector layers into the QGIS project first.                                                             |


For updates, bug reports, and source code:

- **Tracker:** [https://github.com/fnmutua/connector-for-ODK](https://github.com/fnmutua/connector-for-ODK)  
- **Repository:** [https://github.com/fnmutua/ODK-Connect](https://github.com/fnmutua/ODK-Connect)

---

## Screenshot checklist


| Figure | File                            | What to capture                               |
| ------ | ------------------------------- | --------------------------------------------- |
| 1      | `figure1.png`                   | QGIS toolbar + Plugins menu                   |
| 2      | `figure2.png`                   | OSGeo4W Shell pip install (from QGIS install folder) |
| 3      | `figure-2b-stacktrace.png`      | Missing package error banner                  |
| 4      | `figure-2c-missingpackage.png`  | Message log with `ModuleNotFoundError`        |
| 5      | `figure3.png`                   | Plugin Manager search                         |
| 6      | `figure4.png`                   | Install from ZIP tab                          |
| 7      | `figure5.png`                   | Connector for ODK submenu                     |
| 8      | `figrue6-getdata.png`           | Get Data — login screen                       |
| 9      | `figure7.png`                   | Get Data — project/form selected              |
| 10     | `figure8.png`                   | Map with submissions layer                    |
| 11     | `figure9.png`                   | Get Data help panel                           |
| 12     | `figure15.png`                  | Split Layer dialog                            |
| 13     | `figure12a.png`                 | QA/QC — interface                             |
| 14     | `figure12b.png`                 | QA/QC — finished                              |
| 15     | `figure13.png`                  | QA/QC help + template & dictionary links      |
| 16     | `figure14.png`                  | Output folder contents                        |


---

*Connector for ODK v2.0 — Licensed under GPL-3.0*
