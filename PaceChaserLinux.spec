# atrTrailingStop.spec
# -*- mode: python -*-

import sys
from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

# Collect all JSON and icon files
datas = [
    ('user_settings.json', '.'),
    ('tr_history.json', '.'),
    ('stop_history.json', '.'),
    ('atr_history.json', '.'),
]

# Include the linux_assets folder recursively
datas += collect_data_files('linux_assets', include_py_files=False)

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=datas,
    hiddenimports=['utils', 'orders', 'ibkr_api', 'calculator', 'atr_processor'],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='Pace Chaser',
    debug=False,
    strip=False,
    upx=True,
    console=False,  # False if you want GUI only
    icon='linux_assets/assets/icons/hicolor/48x48/PaceChaser.png'
)
