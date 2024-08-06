@echo off
setlocal

rem Step 1: Pull from Git
echo Step 1: Pulling from Git...
# git pull

rem Step 2: Install dependencies
echo Installing dependencies if needed...
@REM pip install -r windows_requirements.txt

rem Step 3: Install playwright
echo Installing playwright
playwright install --with-deps chromium

rem Step 3: Run Python main.py
echo Step 3: Running Python main.py...
python win_main.py

