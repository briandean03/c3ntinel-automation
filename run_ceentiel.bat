@echo off
cd "C:\Users\bd502\internship\python_Automation"
echo ==== %DATE% %TIME% ==== >> log.txt
call "C:\Users\bd502\internship\python_Automation\activate.bat"
python automation.py >> log.txt 2>&1
echo. >> log.txt
