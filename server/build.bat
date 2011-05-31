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

IF NOT EXIST hotchpotch.config (
	ECHO Creating standard configuration
	COPY priv\hotchpotch.config.windows hotchpotch.config
)

SET OPTIONS=debug_info

REM check if we have fuserl available
IF EXIST "%ERL%\lib\erldokan-*" (
	ECHO DOKAN support enabled
	SET OPTIONS=%OPTIONS%, {d, have_dokan}
) ELSE (
	ECHO DOKAN support disabled
)

%ERL%\bin\erl.exe -noshell -eval "make:all([%OPTIONS%]), erlang:halt()"

