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

REM Create store directories...
IF NOT EXIST priv\stores\user MKDIR priv\stores\user
IF NOT EXIST priv\stores\rem1 MKDIR priv\stores\rem1
IF NOT EXIST priv\stores\rem2 MKDIR priv\stores\rem2
IF NOT EXIST priv\stores\sys  MKDIR priv\stores\sys

%ERL%\bin\erl.exe -pa "%CD%\ebin" +A 4 -config hotchpotch -boot start_sasl -s crypto -s hotchpotch

