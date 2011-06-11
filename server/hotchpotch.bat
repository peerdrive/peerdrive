@ECHO OFF

REM Create store directories...
IF NOT EXIST stores\user MKDIR stores\user
IF NOT EXIST stores\sys  MKDIR stores\sys

REM Copy standard configuration if there is none
IF NOT EXIST hotchpotch.config (
	ECHO Creating standard configuration
	COPY templates\hotchpotch.config.windows hotchpotch.config
)

erl.exe -pa "%CD%\applications\hotchpotch\ebin" +A4 +Ww -config hotchpotch ^
  -boot start_sasl -s crypto -s hotchpotch

