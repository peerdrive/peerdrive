@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

REM Create directories...
IF NOT EXIST stores\user MKDIR stores\user
IF NOT EXIST stores\sys  MKDIR stores\sys
IF NOT EXIST vfs  MKDIR vfs

REM Copy standard configuration if there is none
IF NOT EXIST peerdrive.config (
	ECHO Creating standard configuration
	COPY templates\peerdrive.config peerdrive.config
)

REM Configure additional code paths
set ERL_LIBS=apps;deps

erl +A4 +Ww -config peerdrive -boot start_sasl -s crypto -s ssl -s peerdrive
