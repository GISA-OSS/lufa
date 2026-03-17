# Frontend Dependencies (Node.js/npm)

This project uses Node.js and npm exclusively for managing frontend dependencies such as Bootstrap, 
DataTables, and related libraries. **No build system, bundling, or transpilation is used.**

## Purpose and Workflow

The workflow is intentionally simple:
1. **Dependency Declaration**: All frontend libraries are declared in `package.json`
2. **Download**: Running `npm install` downloads the libraries to `node_modules/`
3. **Copy to Distribution**: The script `scripts/copy-assets.sh` copies the minified production files 
from `node_modules/` to `lufa/static/dist/`
4. **Flask Integration**: Flask serves these static files directly from the `dist/` directory

## Benefits

- **Dependency Scanning**: Dependency scanners and security tools can 
automatically analyze `package.json` and identify vulnerabilities, making security management much easier 
than with manually bundled files
- **Version Management**: Clear versioning and update paths for all frontend libraries
- **Reproducibility**: Consistent dependency versions across development, testing, and production environments
- **No Build Complexity**: No webpack, vite, or other build tools required - just simple file copying

## Updating Frontend Dependencies

To update frontend libraries:
```bash
npm update
bash scripts/copy-assets.sh
```

---

[← Back to README](../Readme.md)