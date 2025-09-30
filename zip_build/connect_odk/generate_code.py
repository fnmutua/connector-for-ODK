"""
generate_code.py — Add a unique 'code' field to layers in a File Geodatabase (GDB)

How to run (Windows, recommended):
1) Copy this script to a folder alongside your .gdb (not inside the .gdb).
2) Edit the gdb_path variable below to point to your .gdb folder.
3) Open the OSGeo4W Shell (Start Menu → OSGeo4W Shell). This ensures GDAL/OGR are available.
4) Navigate to the folder containing this script:
   cd C:\\path\\to\\folder
5) Run:
   python generate_code.py

Tips and prerequisites:
- Close applications (like QGIS) that might lock the GDB before running.
- Always keep a backup of your GDB; this script updates features in place.
- If you see an error about importing 'osgeo', you are not in an environment with GDAL/OGR.
  Use the OSGeo4W Shell or install GDAL for your Python environment.
"""

import uuid
import os
import sys

try:
    from osgeo import ogr, gdal
except Exception as e:
    print(
        "Could not import GDAL/OGR (osgeo).\n"
        "Please run this script from the OSGeo4W Shell on Windows, or install GDAL for your Python environment.\n"
        f"Details: {e}"
    )
    sys.exit(1)

# Enable GDAL exceptions
gdal.UseExceptions()

# Path to your File Geodatabase (.gdb) — EDIT THIS VALUE
gdb_path = r"C:\\path\\to\\YourDatabase.gdb"

if not gdb_path or not os.path.exists(gdb_path):
    print(
        "Please edit this script and set 'gdb_path' to your .gdb folder, e.g.:\n"
        r"  gdb_path = r""C:\\data\\MyDatabase.gdb""\n"
        f"Current value: {gdb_path if gdb_path else '<empty>'}"
    )
    sys.exit(2)

# Open the geodatabase
gdb_driver = ogr.GetDriverByName('OpenFileGDB')
if gdb_driver is None:
    print("OpenFileGDB driver not available")
    exit(1)

try:
    gdb = gdb_driver.Open(gdb_path, 1)  # 1 for update mode
    if gdb is None:
        print(f"Could not open geodatabase: {gdb_path}")
        exit(1)
except Exception as e:
    print(f"Error opening geodatabase: {e}")
    exit(1)

# List all layers in the GDB
layer_count = gdb.GetLayerCount()
    print(f"Found {layer_count} layers in geodatabase")

for i in range(layer_count):
    layer = gdb.GetLayerByIndex(i)
    layer_name = layer.GetName()
    print(f"Processing layer: {layer_name}")
    
    # Skip settlement layers
    if 'settlement' in layer_name.lower():
        print(f"Skipping settlement layer: {layer_name}")
        continue
    
    # Get the layer definition
    layer_defn = layer.GetLayerDefn()
    field_count = layer_defn.GetFieldCount()
    
    # Find the code field (exact match, case insensitive)
    code_field_index = -1
    code_field_name = None
    existing_code_field = None
    
    for j in range(field_count):
        field_defn = layer_defn.GetFieldDefn(j)
        field_name = field_defn.GetName()
        if field_name.lower() == 'code':
            if field_name != 'code':
                # Found 'code' field in different case, need to rename it
                existing_code_field = field_name
                print(f"Found field '{field_name}' in different case, will rename to 'code'")
            else:
                # Found exact 'code' field
                code_field_index = j
                code_field_name = field_name
                break
    
    # If we found a field with 'code' in different case, rename it
    if existing_code_field is not None:
        print(f"Renaming field '{existing_code_field}' to 'code' in layer {layer_name}")
        # Create new 'code' field
        field_defn = ogr.FieldDefn('code', ogr.OFTString)
        layer.CreateField(field_defn)
        new_code_field_index = layer_defn.GetFieldCount() - 1
        
        # Copy data from old field to new field
        old_field_index = layer_defn.GetFieldIndex(existing_code_field)
        feature = layer.GetNextFeature()
        while feature is not None:
            old_value = feature.GetField(old_field_index)
            if old_value is not None:
                feature.SetField(new_code_field_index, str(old_value))
            layer.SetFeature(feature)
            feature = layer.GetNextFeature()
        
        # Delete the old field
        layer.DeleteField(old_field_index)
        
        # Refresh layer definition
        layer_defn = layer.GetLayerDefn()
        code_field_index = layer_defn.GetFieldIndex('code')
        code_field_name = 'code'
    
    if code_field_index == -1:
        print(f"No 'code' field found in {layer_name}, creating one...")
        # Create new field
        field_defn = ogr.FieldDefn('code', ogr.OFTString)
        layer.CreateField(field_defn)
        code_field_index = layer_defn.GetFieldCount() - 1
        code_field_name = 'code'
        # Refresh layer definition
        layer_defn = layer.GetLayerDefn()
    
    print(f"Using code field '{code_field_name}' in layer {layer_name}")
    
    # Start transaction
    layer.StartTransaction()
    
    # Generate unique codes for ALL features
    print(f"Generating unique codes for all features...")
    unique_codes = set()
    feature_count = 0
    
    feature = layer.GetNextFeature()
    while feature is not None:
        try:
            # Generate unique UUID
            while True:
                short_uuid = str(uuid.uuid4())[:8]
                if short_uuid not in unique_codes:
                    unique_codes.add(short_uuid)
                    feature.SetField(code_field_index, short_uuid)
                    
                    # Handle geometry to avoid M value encoding issues
                    geom = feature.GetGeometryRef()
                    if geom is not None:
                        # Force 2D geometry to avoid M value issues
                        geom.FlattenTo2D()
                    
                    layer.SetFeature(feature)
                    feature_count += 1
                    break
        except Exception as e:
            print(f"Error updating feature {feature.GetFID()}: {e}")
            # Continue with next feature
            pass
        
        # Get next feature
        feature = layer.GetNextFeature()
    
    # Commit transaction
    layer.CommitTransaction()
    
    print(f"Updated {feature_count} features in layer: {layer_name}")

# Close the geodatabase
gdb = None
print("Processing complete!")
