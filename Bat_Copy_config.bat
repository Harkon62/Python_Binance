@echo on & setlocal
SET src="D:\userData\SynologyDrive\Projekte\Python\Projekt_Python_Binance\config_TA.json"

SET tar="\\wsl$\Debian\home\jf\freqtrade\config.json"

copy /Y %src% %tar%
