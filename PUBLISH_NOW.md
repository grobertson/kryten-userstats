# Publishing kryten-userstats 0.2.0 to PyPI

## Status: ✅ Ready to Publish

The package is built and tested. To publish to PyPI, follow these steps:

## Step 1: Set PyPI Token

You need a PyPI API token. Get one from: https://pypi.org/manage/account/token/

```powershell
# Windows PowerShell
$env:PYPI_TOKEN = "pypi-your-actual-token-here"

# Or set permanently in PowerShell profile
[System.Environment]::SetEnvironmentVariable('PYPI_TOKEN', 'pypi-your-token', 'User')
```

```bash
# Linux/macOS
export PYPI_TOKEN=pypi-your-actual-token-here

# Or add to ~/.bashrc or ~/.zshrc
echo 'export PYPI_TOKEN=pypi-your-token' >> ~/.bashrc
source ~/.bashrc
```

## Step 2: Configure Poetry with Token

```powershell
# Windows PowerShell
C:\Users\me\AppData\Roaming\Python\Scripts\poetry.exe config pypi-token.pypi $env:PYPI_TOKEN
```

```bash
# Linux/macOS
poetry config pypi-token.pypi $PYPI_TOKEN
```

## Step 3: Verify Build

The package is already built in `dist/`:
- `kryten_userstats-0.2.0.tar.gz` (source distribution)
- `kryten_userstats-0.2.0-py3-none-any.whl` (wheel)

Test it was built correctly:
```bash
python -c "import sys; sys.path.insert(0, r'D:\Devel\kryten-userstats'); import userstats; print('Version:', userstats.__version__)"
```

Expected output: `Version: 0.2.0`

## Step 4: Publish to PyPI

```powershell
# Windows PowerShell
cd D:\Devel\kryten-userstats
C:\Users\me\AppData\Roaming\Python\Scripts\poetry.exe publish
```

```bash
# Linux/macOS
cd /path/to/kryten-userstats
poetry publish
```

Or publish with token directly:
```bash
poetry publish --username __token__ --password pypi-your-token-here
```

## Step 5: Verify Publication

After publishing, verify it's available:

1. **Check PyPI page**:
   https://pypi.org/project/kryten-userstats/

2. **Install from PyPI**:
   ```bash
   pip install kryten-userstats
   ```

3. **Test import**:
   ```bash
   python -c "import userstats; print(userstats.__version__)"
   ```

4. **Test CLI**:
   ```bash
   kryten-userstats --help
   ```

## Step 6: Tag Git Release

```bash
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
```

## If You Don't Have PyPI Access

If you don't have PyPI credentials yet:

1. **Create PyPI account**: https://pypi.org/account/register/
2. **Generate API token**: https://pypi.org/manage/account/token/
3. **Copy the token** (it starts with `pypi-`)
4. **Follow steps above** to configure and publish

## Alternative: Test on TestPyPI First

For safety, test on TestPyPI before real PyPI:

```bash
# Configure TestPyPI
poetry config repositories.testpypi https://test.pypi.org/legacy/
poetry config pypi-token.testpypi pypi-your-test-token-here

# Publish to TestPyPI
poetry publish -r testpypi

# Test install
pip install --index-url https://test.pypi.org/simple/ kryten-userstats
```

## Troubleshooting

### Error: "A file named ... already exists"

PyPI doesn't allow overwriting. You must:
1. Bump version in `pyproject.toml`
2. Update `userstats/__init__.py`
3. Rebuild: `poetry build`
4. Publish again

### Error: "Invalid credentials"

Check your token:
```bash
poetry config --list | grep pypi-token
```

Try publishing with explicit credentials:
```bash
poetry publish --username __token__ --password pypi-your-token
```

### Error: "Package not found after publish"

Wait a few minutes for PyPI to index. Then try:
```bash
pip install --no-cache-dir kryten-userstats
```

## Package Information

- **Package Name**: kryten-userstats
- **Version**: 0.2.0
- **Python**: 3.11+
- **License**: MIT
- **Built Files**:
  - Source: `dist/kryten_userstats-0.2.0.tar.gz`
  - Wheel: `dist/kryten_userstats-0.2.0-py3-none-any.whl`

## Post-Publish Checklist

After successful publish:
- [ ] Verify package on PyPI: https://pypi.org/project/kryten-userstats/
- [ ] Test installation: `pip install kryten-userstats`
- [ ] Test import: `python -c "import userstats"`
- [ ] Test CLI: `kryten-userstats --help`
- [ ] Tag release: `git tag v0.2.0`
- [ ] Push tag: `git push origin v0.2.0`
- [ ] Create GitHub release with CHANGELOG
- [ ] Update README badges (if any)
- [ ] Announce release (if desired)

## Support

If you encounter issues:
1. Check Poetry version: `poetry --version` (need 2.0+)
2. Check Python version: `python --version` (need 3.11+)
3. See PUBLISHING.md for detailed troubleshooting
4. See Poetry docs: https://python-poetry.org/docs/

---

**Ready to publish?** Run these commands:

```powershell
# Set your token
$env:PYPI_TOKEN = "pypi-your-token-here"

# Configure Poetry
C:\Users\me\AppData\Roaming\Python\Scripts\poetry.exe config pypi-token.pypi $env:PYPI_TOKEN

# Publish!
C:\Users\me\AppData\Roaming\Python\Scripts\poetry.exe publish
```
