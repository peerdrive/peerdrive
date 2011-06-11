@ECHO OFF
SETLOCAL

%~d0
CD "%~p0\.."

set /p starterl=<releases\start_erl.data
for /f "tokens=1,2 delims= " %%a in ("%starterl%") do set erts_vsn=%%a&set app_vsn=%%b
set erlsrv=erts-%erts_vsn%\bin\erlsrv.exe

REM Always remove the old entry before adding the new one
%erlsrv% remove "Hotchpotch"

REM Register Hotchpotch service
%erlsrv% add "Hotchpotch" ^
  -i hotchpotch ^
  -c "Hotchpotch daemon" ^
  -w "%CD%" ^
  -stopaction "init:stop()." ^
  -debugtype reuse ^
  -args "-boot releases\%app_vsn%\hotchpotch -embedded -config etc\app.config -args_file etc\vm.args"

REM Add dependency to DokanMounter service if installed
reg query HKLM\SYSTEM\CurrentControlSet\Services\DokanMounter /v Start>NUL
IF NOT ERRORLEVEL 1 sc config hotchpotch depend= LanmanWorkstation/DokanMounter

