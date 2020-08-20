REM this script can be used to execute all kinds of commands
goto start
    [usage]
    -> 1. execute the default Main-Class in MF file: .\start
    -> 2. execute the default Main-Class in MF file and pass in one argument: .\start oneArg
    -> 3. execute custom Main-Class: .\start -cp data-adaptor-1.0.jar com.qcm.app.Some

:start


@echo off

set JAVA_OPTS=-Xms8192m -Xmx8192m

cd /d %~dp0
cd ..
SET root=%cd%
@echo %root%
SET libdir=%root%\lib

@echo %libdir%

for /r %libdir% %%f in (data-adaptor*.jar) do (
    SET file=%%f
    goto :end
)

:end

set num=0
for %%a in (%*) do set /a num+=1

ECHO "argument number: " %num%

if %num%==0 (
	java %JAVA_OPTS% -jar %file%
) else if %num%==1 (
	java %JAVA_OPTS% -jar %file% %1
) else if "%1"=="-cp" (
	if %num%==3 (
		java %JAVA_OPTS% %1 %libdir%\%2 %3
	) else (
		ECHO "example:"
		ECHO "	./start -cp xx.jar com.qcm.app.Some"
	)
) else (
	ECHO "unknown arguments"
)

REM if [%1]==[] (
    REM java %JAVA_OPTS% -jar %file%
REM ) else if "%1"=="-cp" (
	REM if [%2] == [] (
		REM ECHO "example:"
		REM ECHO "	./start -cp xx.jar com.qcm.Some"
	REM ) else (
		REM java %JAVA_OPTS% %*
	REM )
REM ) else (
    REM java %JAVA_OPTS% -jar %file% %1
REM )

REM
REM java %JAVA_OPTS% -jar %file%