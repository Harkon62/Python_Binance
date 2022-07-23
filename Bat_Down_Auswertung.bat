@echo off & setlocal
echo %1
echo Start Auswertung loop
start /d D:\userData\SynologyDrive\Projekte\Python\Projekt_Python_Binance /min python3.10 m_evaluation_price.py loop
echo Start Download
python3.10 main.py %1
