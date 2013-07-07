@ECHO OFF
SETLOCAL

%~d0
CD "%~p0\.."

SET APPDIR=
for /F "tokens=1,2,3*" %%i in ('reg query "HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders" /v "Local AppData"') DO (
    if "%%i"=="Local" (
        SET "APPDIR=%%l"
    )
)
if "%APPDIR%"=="" (
	echo "Cannot find Local AppData"
	exit /B 1
)

SET "CONFIG=%APPDIR%\PeerDrive\peerdrive.config"
if exist "%CONFIG%" (
	SET "CONFIG=-config "%CONFIG%""
) else (
	SET CONFIG=
)

set /p starterl=<releases\start_erl.data
for /f "tokens=1,2 delims= " %%a in ("%starterl%") do set erts_vsn=%%a&set app_vsn=%%b

start erts-%erts_vsn%\bin\werl.exe +A 5 -boot releases\%app_vsn%\peerdrive ^
  -embedded %CONFIG%
