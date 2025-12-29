# atrTrailingStop.spec
# -*- mode: python -*-

from PyInstaller.utils.hooks import collect_data_files

block_cipher = None

datas = [
    ('user_settings.json', '.'),
    ('tr_history.json', '.'),
    ('stop_history.json', '.'),
    ('atr_history.json', '.'),
]

datas += collect_data_files('linux_assets', include_py_files=False)
datas += collect_data_files('tzdata', subdir='tzdata')

a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'utils',
        'orders',
        'ibkr_api',
        'calculator',
        'atr_processor',
        'tzdata',
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
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
    console=False,
    icon='linux_assets/assets/icons/hicolor/48x48/PaceChaser.png',
    onefile=True,
)
