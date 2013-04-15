@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@rem This is a mock "Hive" shell that validates whether various test imports
@rem succeeded. It accepts commands of the form 'hive -f scriptname'
@rem and validates that the script contents match those of an expected script.
@rem The filename to that expected script is set via the environment variable
@rem EXPECTED_SCRIPT.

@rem The script will contain a pathname as part of the LOAD DATA INPATH statement;
@rem depending on where you run the tests from, this can change. So the expected
@rem script file actually contains the marker string "BASEPATH" which is replaced
@rem by this script with the contents of $TMPDIR, which is set to 'test.build.data'.

setlocal enabledelayedexpansion

@rem Reset ERRORLEVEL if previously set
set ERRORLEVEL=

if  "%EXPECTED_SCRIPT%" =="" (
 echo No expected script set
 set ERRORLEVEL=1
 goto :end
)

if "%TMPDIR%" == "" (
 echo setting TMPDIR to %TEMP%
 set TMPDIR=%TEMP%
)

if not exist %TMPDIR% (
 md %TMPDIR%
)

if  "%1" NEQ "-f" (
 echo "Misunderstood argument: %1"
 echo "Expected '-f'."
 set ERRORLEVEL=2
 goto :end
)

if  "%2" =="" (
 echo "Expected: hive -f filename"
 set ERRORLEVEL=3
 goto :end
)

set GENERATED_SCRIPT=%2

rem Generate the directory temporarily hosting copied base scripts before processing
set TMP_COPY_DIR=%TMPDIR%\tmp
if not exist %TMP_COPY_DIR% (
 md "%TMP_COPY_DIR%"
)

pushd
rem Adjust path format for TMP_COPY_DIR and TMPDIR
cd /d %TMP_COPY_DIR%
set TMP_COPY_DIR=%cd%

cd /d %TMPDIR%
set TMPDIR=%cd%
popd

@rem Copy the expected script into the tmpdir and replace the marker.
copy "%EXPECTED_SCRIPT%" "%TMP_COPY_DIR%"
for %%i in ("%EXPECTED_SCRIPT%") do set SCRIPT_BASE=%%~nxi
set COPIED_SCRIPT=%TMP_COPY_DIR%\%SCRIPT_BASE%

@rem Replace the BASEPATH marker with actual base-path
set RESOLVED_BASE=/%TMPDIR:\=/%

@rem Trim all comments from copied base scripts
set BASE_SCRIPT=%TMPDIR%\%SCRIPT_BASE%
if exist %BASE_SCRIPT% (
  del %BASE_SCRIPT%
)

@rem Filter all comments from the copied script
FOR /F "usebackq delims=" %%i in (%COPIED_SCRIPT%) do (
  set "line=%%i"
  set "start=!line:~0,2!"
  if not "!start!" == "--" (
    set "resolvedLine=!line:BASEPATH=%RESOLVED_BASE%!"
    echo(!resolvedLine!>>!BASE_SCRIPT!
  )
)

@rem Delete the temporary folder for copied scripts
rd /S /Q %TMP_COPY_DIR%

rem Actually check to see that the input we got matches up.
fc %BASE_SCRIPT% %GENERATED_SCRIPT% /W

:end
echo Exiting with return code %ERRORLEVEL%
exit %ERRORLEVEL%
endlocal
