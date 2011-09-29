@ECHO OFF

REM Create directories...
IF NOT EXIST stores\user MKDIR stores\user
IF NOT EXIST stores\sys  MKDIR stores\sys
IF NOT EXIST vfs  MKDIR vfs

REM Copy standard configuration if there is none
IF NOT EXIST hotchpotch.config (
	ECHO Creating standard configuration
	COPY templates\hotchpotch.config hotchpotch.config
)

REM Configure additional code paths
SET PA=apps\hotchpotch\ebin
FOR /D %%D IN (deps\*) DO SET PA=%PA% %%D\ebin

erl -pa %PA% +A4 +Ww -config hotchpotch -boot start_sasl -s crypto -s hotchpotch

