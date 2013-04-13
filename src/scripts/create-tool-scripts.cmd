@echo off
rem
rem Copyright 2011 The Apache Software Foundation
rem
rem Licensed to the Apache Software Foundation (ASF) under one
rem or more contributor license agreements.  See the NOTICE file
rem distributed with this work for additional information
rem regarding copyright ownership.  The ASF licenses this file
rem to you under the Apache License, Version 2.0 (the
rem "License"); you may not use this file except in compliance
rem with the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

setlocal EnableDelayedExpansion

set outdir=%1
set template=%2
set toollistfile=%3

rem Count the number of lines in the toollist file
set lineCount=0
FOR /F "tokens=*" %%A in (%toollistfile%) do (
  set /A lineCount=!lineCount! + 1
)
set /A lastCommand=%lineCount% - 1

rem Create a tool-script for each tool
set currentLine=0
FOR /F "tokens=*" %%A in (%toollistfile%) do (
  call :parseCommand SUBCOMMANDMARKER %%A
)

rem For each line in the template file, replace token [1], with token [2]
rem and write the updated script as the target tool script
:parseCommand
if %currentLine% GTR 1 if %currentLine% LSS %lastCommand% (
  rem Get the script name for the current tool
  set toolScriptTarget=%outdir%\sqoop-%2.cmd

  rem Replace the source token with target token, and write the result to
  rem target script
  for /f "tokens=*" %%i in (%template%) do (
	set line=%%i
	echo !line:%1=%2!>> !toolScriptTarget!
  )
)
set /A currentLine=%currentLine%+1
goto :eof

endlocal