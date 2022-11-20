@echo off & setlocal
echo %1
echo Start Auswertung loop
cmd /k python3.10 m_evaluation_price.py loop
