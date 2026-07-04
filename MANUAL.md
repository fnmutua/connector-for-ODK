# Connector for ODK — User Manuals

Two manuals are available for plugin version 2.0:

| Manual | File | PDF | Audience |
| ------ | ---- | --- | -------- |
| **Version 1 — Administrator** | `MANUAL_v1_admin.md` | `Connector_for_ODK_User_Manual_v1_Admin.pdf` | Includes KeSMIS Import (upload) |
| **Version 2 — Standard** | `MANUAL_v2.md` | `Connector_for_ODK_User_Manual_v2.pdf` | Get Data, Split Layer, and QA/QC only |

Build both PDFs:

```bash
python build_manual_pdf.py
```

Build one edition:

```bash
python build_manual_pdf.py --edition admin
python build_manual_pdf.py --edition standard
```
