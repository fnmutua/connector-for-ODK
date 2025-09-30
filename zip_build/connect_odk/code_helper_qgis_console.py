"""
QGIS Python Console helper: ensure a 'code' text field exists on all editable vector
layers (skipping names containing 'settlement'), populate missing values with unique
8-character UUIDs, and commit per layer.

How to run inside QGIS:
- Open QGIS with your project and layers loaded
- Open Python Console (Plugins → Python Console)
- Click the Editor button and open this file, then Run, or copy/paste its contents
"""

from qgis.core import (
    QgsProject, QgsVectorLayer, QgsField, QgsVectorDataProvider
)
from qgis.PyQt.QtCore import QVariant
import uuid


def ensure_edit_mode(layer):
    if not layer.isEditable():
        if not layer.startEditing():
            raise RuntimeError(f"Could not start editing for layer: {layer.name()}")


def try_rename_attribute(layer, field_index, new_name):
    try:
        caps = layer.dataProvider().capabilities()
        if caps & QgsVectorDataProvider.RenameAttributes:
            if not layer.renameAttribute(field_index, new_name):
                return False
            return True
        return False
    except Exception:
        return False


def add_field(layer, name, qvariant_type=QVariant.String):
    caps = layer.dataProvider().capabilities()
    if not (caps & QgsVectorDataProvider.AddAttributes):
        raise RuntimeError(f"Provider does not allow adding attributes for layer: {layer.name()}")
    fld = QgsField(name, qvariant_type)
    if not layer.addAttribute(fld):
        raise RuntimeError(f"Failed to add field '{name}' to layer: {layer.name()}")
    layer.updateFields()
    return layer.fields().indexOf(name)


def delete_field(layer, field_index):
    caps = layer.dataProvider().capabilities()
    if caps & QgsVectorDataProvider.DeleteAttributes:
        layer.deleteAttribute(field_index)
        layer.updateFields()


def collect_existing_codes(layer, code_idx):
    existing = set()
    for f in layer.getFeatures():
        val = f[code_idx]
        if val is not None and str(val).strip() != "":
            existing.add(str(val))
    return existing


def generate_unique_code(existing):
    while True:
        c = str(uuid.uuid4())[:8]
        if c not in existing:
            existing.add(c)
            return c


def process_layer(layer):
    if not isinstance(layer, QgsVectorLayer):
        print(f"Skipping non-vector layer: {layer.name()}")
        return

    if not layer.isValid() or layer.readOnly():
        print(f"Skipping invalid/readonly layer: {layer.name()}")
        return

    if 'settlement' in layer.name().lower():
        print(f"Skipping settlement layer: {layer.name()}")
        return

    caps = layer.dataProvider().capabilities()
    if not (caps & (QgsVectorDataProvider.AddAttributes | QgsVectorDataProvider.ChangeAttributeValues)):
        print(f"Skipping layer without needed caps: {layer.name()}")
        return

    print(f"\nProcessing layer: {layer.name()}")
    ensure_edit_mode(layer)

    fields = layer.fields()
    field_names = [f.name() for f in fields]
    lower_to_index = {name.lower(): idx for idx, name in enumerate(field_names)}
    code_idx = -1

    if 'code' in lower_to_index:
        idx = lower_to_index['code']
        actual_name = field_names[idx]
        if actual_name != 'code':
            if try_rename_attribute(layer, idx, 'code'):
                layer.updateFields()
                code_idx = layer.fields().indexOf('code')
                print(f"Renamed field '{actual_name}' -> 'code'")
            else:
                print(f"Could not rename '{actual_name}'. Creating 'code' and copying values...")
                new_idx = add_field(layer, 'code', QVariant.String)
                for f in layer.getFeatures():
                    layer.changeAttributeValue(f.id(), new_idx, f[idx])
                delete_field(layer, idx)
                layer.updateFields()
                code_idx = layer.fields().indexOf('code')
        else:
            code_idx = idx
            print("Found existing 'code' field.")
    else:
        code_idx = add_field(layer, 'code', QVariant.String)
        print("Created 'code' field.")

    if code_idx < 0:
        raise RuntimeError(f"Could not locate/create 'code' field for layer: {layer.name()}")

    existing_codes = collect_existing_codes(layer, code_idx)

    updated = 0
    for f in layer.getFeatures():
        val = f[code_idx]
        if val is None or str(val).strip() == "":
            new_code = generate_unique_code(existing_codes)
            layer.changeAttributeValue(f.id(), code_idx, new_code)
            updated += 1

    if updated > 0:
        if not layer.commitChanges():
            layer.rollBack()
            raise RuntimeError(f"Commit failed for layer: {layer.name()} — {layer.commitErrors()}")
        print(f"Updated {updated} features and saved edits for: {layer.name()}")
    else:
        if layer.isEditable():
            layer.commitChanges()
        print(f"No missing codes. Nothing to update for: {layer.name()}")


def main():
    layers = list(QgsProject.instance().mapLayers().values())
    if not layers:
        print("No layers in project.")
        return

    processed = 0
    for lyr in layers:
        try:
            process_layer(lyr)
            processed += 1
        except Exception as e:
            print(f"Error on layer '{lyr.name()}': {e}")

    print(f"\nDone. Processed {processed} layer(s).")


if __name__ == "__main__":
    main()


