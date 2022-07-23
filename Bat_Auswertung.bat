@echo off & setlocal
echo %1
REM start /d D:\...\Projekte\Python\Projekt_Python_Binance /min python3.10 m_evaluation_price.py loop
python3.10 m_evaluation_price.py %1 %2
pause
