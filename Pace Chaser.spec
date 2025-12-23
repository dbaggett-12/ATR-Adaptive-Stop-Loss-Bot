# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT, BUNDLE

# Data files
datas = [
    ('assets', 'assets'),
    ('user_settings.json', '.'),
    ('tr_history.json', '.'),
    ('stop_history.json', '.'),
]

hiddenimports = []

# ---------------------------
# Analysis
# ---------------------------
a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# ---------------------------
# EXE
# ---------------------------
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,  # binaries will be included in COLLECT
    name='Pace Chaser',
    debug=False,
    strip=False,
    upx=True,
    console=False,
)

# ---------------------------
# COLLECT everything
# ---------------------------
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name='Pace Chaser',
)

# ---------------------------
# BUNDLE into .app
# ---------------------------
app = BUNDLE(
    coll,
    name='Pace Chaser.app',
    icon='assets/PaceChaser.icns',
    bundle_identifier=None,
)
