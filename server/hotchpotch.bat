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

erl.exe -pa "%CD%\apps\hotchpotch\ebin" +A4 +Ww -config hotchpotch ^
  -boot start_sasl -s crypto -s hotchpotch

