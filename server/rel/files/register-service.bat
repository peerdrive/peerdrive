@ECHO OFF
SETLOCAL

%~d0
CD "%~p0\.."

set /p starterl=<releases\start_erl.data
for /f "tokens=1,2 delims= " %%a in ("%starterl%") do set erts_vsn=%%a&set app_vsn=%%b
set erlsrv=erts-%erts_vsn%\bin\erlsrv.exe

REM Query path of config file
SET APPDIR=
for /F "tokens=1,2,3*" %%i in ('reg query "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders" /v "Common AppData"') DO (
    if "%%i"=="Common" (
        SET "APPDIR=%%l"
    )
)
if "%APPDIR%"=="" (
	echo "Cannot find Common AppData"
	exit /B 1
)

REM Create empty config file
SET "CONFIG=%APPDIR%\PeerDrive\peerdrive.config"
if not exist "%CONFIG%" (
	mkdir "%APPDIR%\PeerDrive"
	copy etc\app.config "%CONFIG%"
)

REM Always remove the old entry before adding the new one
%erlsrv% remove "PeerDrive"

REM Register PeerDrive service
%erlsrv% add "PeerDrive" ^
  -i peerdrive ^
  -c "PeerDrive daemon" ^
  -w "%CD%" ^
  -stopaction "init:stop()." ^
  -args "-boot releases\%app_vsn%\peerdrive -embedded -config ""%CONFIG%"" -args_file etc\vm.args -system"

REM Add dependency to DokanMounter service if installed
reg query HKLM\SYSTEM\CurrentControlSet\Services\DokanMounter /v Start>NUL
IF NOT ERRORLEVEL 1 sc config peerdrive depend= LanmanWorkstation/DokanMounter

REM Start service immediately
sc start peerdrive

