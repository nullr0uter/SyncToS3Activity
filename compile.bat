@echo off
chcp 65001
echo Starte Kompilierung
nuitka --output-dir=build --follow-imports SyncToS3Activity.py
pause