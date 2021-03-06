@echo off

set JAVA_OPTS=-Xms1024m -Xmx1024m

cd /d %~dp0
cd ..
SET root=%cd%
REM @echo %root%
SET libdir=%root%\lib

REM @echo %libdir%

for /r %libdir% %%f in (data-adaptor*.jar) do (
    SET file=%%f
    goto :end
)

:end
REM if [%1]==[] (java -jar %file%) else (java -jar %file% %1)
java %JAVA_OPTS% -jar %file% %1