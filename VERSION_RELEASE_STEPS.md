# Steps to Release Next Version (1.6)

## Version Updated
✅ Version updated from **1.5** to **1.6** in `metadata.txt`

## Next Steps to Release

### Option 1: Using pb_tool (Recommended)

1. **Compile the plugin**:
   ```bash
   pb_tool compile
   ```

2. **Create ZIP package**:
   ```bash
   pb_tool zip
   ```
   This will create `connect_odk.zip` in your current directory.

3. **Deploy to QGIS** (optional, for testing):
   ```bash
   pb_tool deploy
   ```

### Option 2: Using Makefile

1. **Compile resources**:
   ```bash
   make compile
   ```

2. **Create package**:
   ```bash
   make zip
   ```

### Option 3: Manual Git Tagging (if using Git)

1. **Commit your changes**:
   ```bash
   git add metadata.txt
   git commit -m "Bump version to 1.6"
   ```

2. **Create a version tag**:
   ```bash
   git tag -a v1.6 -m "Release version 1.6"
   git push origin v1.6
   ```

3. **Create package from tag**:
   ```bash
   make package VERSION=v1.6
   ```

## Verification Checklist

- [x] Version updated in `metadata.txt` (1.5 → 1.6)
- [ ] Resources compiled (`resources.py` exists)
- [ ] ZIP package created
- [ ] Plugin tested in QGIS
- [ ] Git tag created (if using version control)
- [ ] Ready for distribution

## Distribution

The ZIP file can be:
- Uploaded to QGIS Plugin Repository
- Shared directly with users
- Deployed to QGIS plugins directory for testing

## Notes

- The version number follows semantic versioning (major.minor.patch)
- For patch releases (bug fixes): increment patch (1.6.0 → 1.6.1)
- For minor releases (new features): increment minor (1.6 → 1.7)
- For major releases (breaking changes): increment major (1.6 → 2.0)
