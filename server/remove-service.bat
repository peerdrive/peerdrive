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

"%ERTS%\bin\erlsrv" remove "Hotchpotch"
