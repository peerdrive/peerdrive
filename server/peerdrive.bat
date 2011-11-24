@ECHO OFF

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
SET PA=apps\peerdrive\ebin
FOR /D %%D IN (deps\*) DO SET PA=%PA% %%D\ebin

start werl -pa %PA% +A4 +Ww -config peerdrive -boot start_sasl -s crypto -s peerdrive
