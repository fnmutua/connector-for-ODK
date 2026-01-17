# Steps to Compile QGIS Plugin using pb_tool

## Prerequisites

1. **Install pb_tool** (if not already installed):
   ```bash
   pip install pb_tool
   ```
   
   Or from the URL mentioned in pb_tool.cfg:
   ```bash
   pip install http://geoapt.net/files/pb_tool.zip
   ```

## Compilation Steps

### Step 1: Navigate to Plugin Directory
Open a terminal/command prompt and navigate to your plugin directory:
```bash
cd C:\Users\Administrator\ODK-Connect
```

### Step 2: Compile Resources
Compile the resource file (`resources.qrc`) to generate `resources.py`:
```bash
pb_tool compile
```

This will:
- Compile `resources.qrc` → `resources.py`
- Process all resource files (SVG icons) defined in the .qrc file

### Step 3: Verify Compilation
Check that `resources.py` was created:
```bash
dir resources.py
```

## Additional pb_tool Commands

### Deploy Plugin to QGIS
Deploy the compiled plugin to your QGIS plugins directory:
```bash
pb_tool deploy
```

### Create Plugin ZIP Package
Create a distributable ZIP file:
```bash
pb_tool zip
```

### Validate Plugin
Check plugin configuration:
```bash
pb_tool validate
```

### List Available Commands
See all available pb_tool commands:
```bash
pb_tool --help
```

## Manual Compilation (Alternative)

If pb_tool is not available, you can manually compile resources using:

```bash
pyrcc5 -o resources.py resources.qrc
```

This requires PyQt5 tools to be installed:
```bash
pip install PyQt5
```

## Notes

- The main dialog file (`connect_odk_dialog_base.ui`) is loaded directly and doesn't need compilation
- Only `resources.qrc` needs to be compiled according to your `pb_tool.cfg`
- After compilation, `resources.py` will be generated in the root directory
