@ECHO OFF
SETLOCAL

%~d0
CD "%~p0\.."

set /p starterl=<releases\start_erl.data
for /f "tokens=1,2 delims= " %%a in ("%starterl%") do set erts_vsn=%%a&set app_vsn=%%b

erts-%erts_vsn%\bin\erlsrv.exe remove "PeerDrive"

TASKKILL /F /IM epmd.exe

