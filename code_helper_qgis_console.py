"""
QGIS helper: ensure a 'code' text field exists on editable vector layers,
populate missing values with unique 8-character UUIDs, and commit in the
current QGIS session.
"""

import uuid

from qgis.core import (
    QgsProject,
    QgsVectorLayer,
    QgsField,
    QgsVectorDataProvider,
    edit,
)
from qgis.PyQt.QtCore import QVariant


def is_parent_boundary_layer(layer_name):
    """Skip reference boundary layers loaded for spatial matching, not data layers."""
    return layer_name.strip().endswith(" Boundaries")


def is_settlement_data_layer(layer_name):
    """Detect user data layers whose name indicates settlements (not boundary reference layers)."""
    if is_parent_boundary_layer(layer_name):
        return False
    return "settlement" in layer_name.lower()


def describe_edit_blockers(layer):
    """Return provider details and missing capabilities for clearer logs."""
    provider = layer.dataProvider()
    caps = provider.capabilities()
    missing = []
    if not (caps & QgsVectorDataProvider.AddAttributes):
        missing.append("AddAttributes")
    if not (caps & QgsVectorDataProvider.ChangeAttributeValues):
        missing.append("ChangeAttributeValues")
    return provider.name(), provider.dataSourceUri(), missing


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


def process_layer(layer, log=None):
    """
    Add/fill the 'code' field on a vector layer and save edits in session.

    Returns True when the layer was updated and saved, False when skipped.
    """
    if log is None:
        log = print

    if not isinstance(layer, QgsVectorLayer):
        log(f"Skipping non-vector layer: {layer.name()}")
        return False

    if not layer.isValid():
        log(f"Skipping invalid layer: {layer.name()}")
        return False

    if is_parent_boundary_layer(layer.name()):
        log(f"Skipping parent boundary layer: {layer.name()}")
        return False

    if is_settlement_data_layer(layer.name()):
        log(
            f"Skipping settlement layer: {layer.name()}. "
            "Use Sync Settlement Codes from KeSMIS instead of generating UUID codes."
        )
        return False

    log(f"Processing layer: {layer.name()}")

    try:
        with edit(layer):
            fields = layer.fields()
            field_names = [f.name() for f in fields]
            lower_to_index = {name.lower(): idx for idx, name in enumerate(field_names)}
            code_idx = -1

            if "code" in lower_to_index:
                idx = lower_to_index["code"]
                actual_name = field_names[idx]
                if actual_name != "code":
                    if try_rename_attribute(layer, idx, "code"):
                        layer.updateFields()
                        code_idx = layer.fields().indexOf("code")
                        log(f"Renamed field '{actual_name}' -> 'code'")
                    else:
                        log(f"Could not rename '{actual_name}'. Creating 'code' and copying values...")
                        new_idx = add_field(layer, "code", QVariant.String)
                        for f in layer.getFeatures():
                            layer.changeAttributeValue(f.id(), new_idx, f[idx])
                        delete_field(layer, idx)
                        layer.updateFields()
                        code_idx = layer.fields().indexOf("code")
                else:
                    code_idx = idx
                    log("Found existing 'code' field.")
            else:
                code_idx = add_field(layer, "code", QVariant.String)
                log("Created 'code' field.")

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
                log(f"Filled {updated} missing code value(s) for: {layer.name()}")
            else:
                log(f"All features already had codes in: {layer.name()}")

    except Exception as e:
        provider_name, source_uri, missing = describe_edit_blockers(layer)
        log(f"Could not save 'code' for layer '{layer.name()}': {e}")
        if missing:
            log(f"Provider: {provider_name}, missing capabilities: {', '.join(missing)}")
        log(f"Source: {source_uri}")
        return False

    layer.updateFields()
    provider = layer.dataProvider()
    if hasattr(provider, "reloadData"):
        provider.reloadData()

    log(f"Saved 'code' field for layer: {layer.name()}")
    return True


def main(log=None):
    if log is None:
        log = print

    layers = list(QgsProject.instance().mapLayers().values())
    if not layers:
        log("No layers in project.")
        return

    saved = 0
    skipped = 0
    for lyr in layers:
        try:
            if process_layer(lyr, log=log):
                saved += 1
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            log(f"Error on layer '{lyr.name()}': {e}")

    log(f"Done. Saved on {saved} layer(s), skipped {skipped}.")


if __name__ == "__main__":
    main()
