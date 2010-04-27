@ECHO OFF

IF DEFINED ERL GOTO KnowErl

REM Trying to find the erlang executable...
FOR /D %%D IN (%ProgramFiles%\erl*) DO IF EXIST "%%D\bin\erl.exe" SET ERL="%%D\bin"
IF DEFINED ERL GOTO KnowErl

ECHO Could not find your erlang installation. Please set the ERL environment
ECHO variable to the erlang executables directory, e.g. ERL=C:\Erlang\bin.
ECHO Use quotes if the path contains spaces!
PAUSE
GOTO :EOF

:KnowErl

ECHO Create store directories...
IF NOT EXIST priv\stores\user MKDIR priv\stores\user
IF NOT EXIST priv\stores\rem1 MKDIR priv\stores\rem1
IF NOT EXIST priv\stores\rem2 MKDIR priv\stores\rem2
IF NOT EXIST priv\stores\sys  MKDIR priv\stores\sys

ECHO Copy standard application template if it does not exist yet
IF NOT EXIST ebin\hotchpotch.app COPY priv\hotchpotch.app.windows ebin\hotchpotch.app

ECHO Compile all source files...
%ERL%\erl.exe -noshell -eval "case make:all([{d, windows}]) of up_to_date -> erlang:halt(); error -> erlang:halt(1) end"
IF NOT ERRORLEVEL 0 (
	ECHO Build errors...
	PAUSE
)

ECHO Start the server, possibly as daemon...
IF "%1" == "detach" (
	set CMD=%ERL%\erl.exe -config daemon.config -detached -name hotchpotch@127.0.0.1 -setcookie hotchpotch
) ELSE (
	set CMD=%ERL%\werl.exe
)
%CMD% -pa "%CD%\ebin" +A 4 -boot start_sasl -s crypto -s hotchpotch

