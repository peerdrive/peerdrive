@ECHO OFF

IF DEFINED ERL GOTO KnowErl

REM Trying to find the erlang executable...
FOR /D %%D IN ("%ProgramFiles%\erl*") DO IF EXIST "%%D\bin\erl.exe" SET ERL=%%D
IF DEFINED ERL GOTO KnowErl

ECHO Could not find your erlang installation. Please set the ERL environment
ECHO variable to the erlang installation directory, e.g. ERL=C:\Erlang.
PAUSE
GOTO :EOF

:KnowErl

FOR /D %%D IN ("%ERL%\erts-*") DO IF EXIST "%%D\bin\erlsrv.exe" SET ERTS=%%D

REM Always remove the old entry before adding the new one
"%ERTS%\bin\erlsrv" remove "Hotchpotch"
"%ERTS%\bin\erlsrv" add "Hotchpotch" ^
  -i hotchpotch ^
  -c "Hotchpotch daemon" ^
  -w "%CD%" ^
  -stopaction "init:stop()." ^
  -debugtype reuse ^
  -args "+A4 +Ww -pa ebin -config hotchpotch -boot start_sasl -s crypto -s hotchpotch"

REM Add dependency to DokanMounter service if installed
reg query HKLM\SYSTEM\CurrentControlSet\Services\DokanMounter /v Start>NUL
IF NOT ERRORLEVEL 1 sc config hotchpotch depend= LanmanWorkstation/DokanMounter

