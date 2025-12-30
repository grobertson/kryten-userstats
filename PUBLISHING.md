# Publishing to PyPI

This guide covers publishing kryten-userstats to PyPI using Poetry.

## Prerequisites

1. **PyPI Account**: Create an account at https://pypi.org
2. **API Token**: Generate an API token at https://pypi.org/manage/account/token/
3. **Poetry Installed**: Version 2.2.0 or higher

## One-Time Setup

### 1. Configure PyPI Token

**Option A: Environment Variable (Recommended)**
```bash
# Linux/macOS
export PYPI_TOKEN=pypi-your-token-here

# Windows PowerShell
$env:PYPI_TOKEN = "pypi-your-token-here"
```

**Option B: Poetry Config**
```bash
poetry config pypi-token.pypi pypi-your-token-here
```

### 2. Verify Configuration

```bash
poetry config --list | grep pypi-token
```

## Publishing Process

### 1. Update Version

Edit `pyproject.toml`:
```toml
[tool.poetry]
version = "0.2.0"  # Update this
```

Or use Poetry:
```bash
# Bump patch version (0.2.0 -> 0.2.1)
# Use pyproject.toml to manage version patch

# Bump minor version (0.2.0 -> 0.3.0)
# Use pyproject.toml to manage version minor

# Bump major version (0.2.0 -> 1.0.0)
# Use pyproject.toml to manage version major
```

### 2. Update CHANGELOG.md

Add entry for new version with changes.

### 3. Update __init__.py

```python
__version__ = "0.2.0"  # Match pyproject.toml
```

### 4. Clean Previous Builds

```bash
# Remove old builds
rm -rf dist/

# Or on Windows
Remove-Item -Recurse -Force dist
```

### 5. Build Package

```bash
uv build
```

This creates:
- `dist/kryten_userstats-0.2.0.tar.gz` (source distribution)
- `dist/kryten_userstats-0.2.0-py3-none-any.whl` (wheel)

### 6. Test Package Locally

```bash
# Install in editable mode
pip install -e .

# Or test the built wheel
pip install dist/kryten_userstats-0.2.0-py3-none-any.whl

# Test import
python -c "import userstats; print(userstats.__version__)"

# Test CLI
kryten-userstats --help
```

### 7. Test on TestPyPI (Optional but Recommended)

```bash
# Configure TestPyPI token
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config pypi-token.testpypi pypi-your-test-token-here

# Publish to TestPyPI
uv publish -r testpypi

# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ kryten-userstats

# Test the installation
python -c "import userstats; print(userstats.__version__)"
```

### 8. Publish to PyPI

```bash
# Using configured token
uv publish

# Or specify token inline
uv publish --username __token__ --password pypi-your-token-here
```

### 9. Verify Publication

```bash
# Check on PyPI
https://pypi.org/project/kryten-userstats/

# Install from PyPI
pip install kryten-userstats

# Test
python -c "import userstats; print(userstats.__version__)"
kryten-userstats --help
```

### 10. Tag Release on Git

```bash
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
```

## Automated Publishing (GitHub Actions)

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install Poetry
        run: |
          curl -sSL https://install.python-poetry.org | python3 -
          echo "$HOME/.local/bin" >> $GITHUB_PATH
      
      - name: Build and publish
        env:
          POETRY_PYPI_TOKEN_PYPI: ${{ secrets.PYPI_TOKEN }}
        run: |
          uv build
          uv publish
```

Then:
1. Add `PYPI_TOKEN` to GitHub repository secrets
2. Create a release on GitHub
3. Package is automatically published

## Troubleshooting

### Build Fails

**Check dependencies:**
```bash
uv pip check
```

**Update lock file:**
```bash
uv lock --no-update
```

### Upload Fails

**Check credentials:**
```bash
poetry config --list | grep pypi-token
```

**Try with username/password:**
```bash
uv publish --username __token__ --password pypi-your-token-here
```

### Version Already Exists

PyPI doesn't allow overwriting versions. You must:
1. Bump version number
2. Build again
3. Publish new version

### Import Fails After Installation

**Check package name:**
- PyPI name: `kryten-userstats` (with hyphen)
- Import name: `userstats` (underscore in package, but just the module name)

```python
# Correct
import userstats

# Wrong
import kryten-userstats
```

## Version Numbering Guidelines

Follow [Semantic Versioning](https://semver.org/):

- **MAJOR** (1.0.0): Breaking changes, incompatible API changes
- **MINOR** (0.1.0): New features, backward compatible
- **PATCH** (0.0.1): Bug fixes, backward compatible

Examples:
- `0.1.0` → `0.1.1`: Bug fix
- `0.1.1` → `0.2.0`: New feature (metrics server)
- `0.9.0` → `1.0.0`: Stable release, API finalized

## Pre-release Versions

For testing:
```bash
# Use pyproject.toml to manage version prerelease  # 0.2.0 -> 0.2.1-alpha.0
# Use pyproject.toml to manage version prerelease  # 0.2.1-alpha.0 -> 0.2.1-alpha.1
```

For release candidates:
```bash
# Manually in pyproject.toml
version = "0.2.0-rc.1"
```

## Current Release Status

**Latest Version**: 0.2.0  
**Published**: 2025-12-05  
**PyPI**: https://pypi.org/project/kryten-userstats/  

## Quick Publish Checklist

- [ ] Update version in `pyproject.toml`
- [ ] Update version in `userstats/__init__.py`
- [ ] Update `CHANGELOG.md`
- [ ] Clean `dist/` directory
- [ ] Run `uv build`
- [ ] Test locally: `pip install dist/*.whl`
- [ ] Test CLI: `kryten-userstats --help`
- [ ] Test import: `python -c "import userstats"`
- [ ] Publish: `uv publish`
- [ ] Verify on PyPI
- [ ] Tag release: `git tag v0.2.0`
- [ ] Push tag: `git push origin v0.2.0`
