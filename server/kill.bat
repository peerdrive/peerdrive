@ECHO OFF

IF DEFINED ERL GOTO KnowErl

REM Try to find the erlang executable
FOR /D %%D IN (%ProgramFiles%\erl*) DO IF EXIST "%%D\bin\erl.exe" SET ERL="%%D\bin"
IF DEFINED ERL GOTO KnowErl

ECHO Could not find your erlang installation. Please set the ERL environment
ECHO variable to the erlang executables directory, e.g. ERL=C:\Erlang\bin.
ECHO Use quotes if the path contains spaces!
PAUSE
GOTO :EOF

:KnowErl

%ERL%\erl.exe -name killer@127.0.0.1 -setcookie hotchpotch -eval "rpc:call('hotchpotch@127.0.0.1', init, stop, []), init:stop()."
