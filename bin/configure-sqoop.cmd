:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

if not exist %bin% (
  echo Error: Environment variable bin not defined.
  echo This is generally because this script should not be invoked directly. Use sqoop instead.
  exit /b 1
)

if not defined SQOOP_HOME (
  set SQOOP_HOME=%bin%\..
)

if not defined SQOOP_CONF_DIR (
  set SQOOP_CONF_DIR=%SQOOP_HOME%\conf
)

:: Call sqoop-env if it exists under SQOOP_CONF_DIR
if exist %SQOOP_CONF_DIR%\sqoop-env.cmd (
  call %SQOOP_CONF_DIR%\sqoop-env.cmd
)

:: Find paths to our dependency systems. If they are unset, use CDH defaults.

if not defined HADOOP_COMMON_HOME (
  if defined HADOOP_HOME (
    set HADOOP_COMMON_HOME=%HADOOP_HOME%
  ) else (
    :: Check: If we can't find our dependencies, give up here.
    echo Error: The environment variable HADOOP_HOME has not been defined.
    echo Please set HADOOP_HOME to the root of your Hadoop installation.
    exit /b 1
  )
)
if not defined HADOOP_MAPRED_HOME (
  if defined HADOOP_HOME (
    set HADOOP_MAPRED_HOME=%HADOOP_HOME%
  ) else (
    :: Check: If we can't find our dependencies, give up here.
    echo Error: The environment variable HADOOP_HOME has not been defined.
    echo Please set HADOOP_HOME to the root of your Hadoop installation.
    exit /b 1
  )
)

:: We are setting HADOOP_HOME to HADOOP_COMMON_HOME if it is not set
:: so that hcat script works correctly on BigTop
if not defined HADOOP_HOME (
  if defined HADOOP_COMMON_HOME (
    set HADOOP_HOME=%HADOOP_COMMON_HOME%
  )
)

:: Check for HBase dependency
if not defined HBASE_HOME (
  if defined HBASE_VERSION (
    set HBASE_HOME=%HADOOP_HOME%\..\hbase-%HBASE_VERSION%
  ) else (
    echo Warning: HBASE_HOME and HBASE_VERSION not set.
  )
)
:: Check for HCatalog dependency
if not defined HCAT_HOME (
  if defined HCATALOG_HOME (
     set HCAT_HOME=%HCATALOG_HOME%
  ) else (
     echo Warning: HCAT_HOME not set
  )
)

if not exist "%HCATALOG_HOME%" (
  echo Warning: HCATALOG_HOME does not exist! HCatalog imports will fail.
  echo Please set HCATALOG_HOME to the root of your HCatalog installation.
)
 
::
:: Check for Accumulo dependency
if not defined ACCUMULO_HOME (
  echo Warning: ACCUMULO_HOME not set.
)
if not defined ZOOKEEPER_HOME (
  echo Warning: ZOOKEEPER_HOME not set.
)

:: Check: If we can't find our dependencies, give up here.

:: Check: If HADOOP_COMMON_HOME path actually exists
if not exist %HADOOP_COMMON_HOME% (
  echo Error: HADOOP_COMMON_HOME does not exist!
  echo Please set HADOOP_COMMON_HOME to the root of your Hadoop installation.
  exit /b 1
)
:: Check: If HADOOP_MAPRED_HOME path actually exists
if not exist %HADOOP_MAPRED_HOME% (
  echo Error: HADOOP_MAPRED_HOME does not exist!
  echo Please set HADOOP_MAPRED_HOME to the root of your Hadoop installation.
  exit /b 1
)
if not exist "%HBASE_HOME%" (
  echo Warning: HBASE_HOME does not exist! HBase imports will fail.
  echo Please set HBASE_HOME to the root of your HBase installation.
)
if not exist "%ACCUMULO_HOME%" (
  echo Warning: ACCUMULO_HOME does not exist! Accumulo imports will fail.
  echo Please set ACCUMULO_HOME to the root of your Accumulo installation.
)
if not exist "%ZOOKEEPER_HOME%" (
  echo Warning: ZOOKEEPER_HOME does not exist! Accumulo imports will fail.
  echo Please set ZOOKEEPER_HOME to the root of your Zookeeper installation.
)

:: Add sqoop dependencies to classpath
set SQOOP_CLASSPATH=

:: Where to find the main Sqoop jar
set SQOOP_JAR_DIR=%SQOOP_HOME%

:: If there's a "build" subdir, override with this, so we use
:: the newly-compiled copy.
if exist "%SQOOP_JAR_DIR%\build" (
  set SQOOP_JAR_DIR=%SQOOP_JAR_DIR%\build
)
call :add_dir_to_classpath %SQOOP_JAR_DIR%

if exist "%SQOOP_HOME%\lib" (
  call :add_dir_to_classpath %SQOOP_HOME%\lib
)

:: Add HCatalog Home to the dependency list so that newer thrift libraries are
:: used instead of HBase version

if exist "%HCATALOG_HOME%" (
  if defined PYTHON_CMD (
     for /F "usebackq eol==" %%G IN ( `%PYTHON_CMD% %HCAT_HOME%\bin\hcat.py -classpath`) DO SET SQOOP_CLASSPATH=%SQOOP_CLASSPATH%;%%G
  ) else (
     for /f "usebackq eol==" %%G IN ( `python %HCAT_HOME%\bin\hcat.py -classpath`) DO SET SQOOP_CLASSPATH=%SQOOP_CLASSPATH%;%%G
  )
)


:: Add HBase to dependency list
if exist "%HBASE_HOME%" (
  call :add_dir_to_classpath %HBASE_HOME%
  call :add_dir_to_classpath %HBASE_HOME%\lib
  set SQOOP_CLASSPATH=!SQOOP_CLASSPATH!;%HBASE_HOME%\conf
 )
 
::
:: Add Accumulo to dependency list
if exist "%ACCUMULO_HOME%" (
  call :add_dir_to_classpath %ACCUMULO_HOME%
  call :add_dir_to_classpath %ACCUMULO_HOME%\lib
)

if not defined ZOOCFGDIR (
  if defined ZOOKEEPER_CONF_DIR (
    set ZOOCFGDIR=%ZOOKEEPER_CONF_DIR%
  ) else (
  if defined ZOOKEEPER_HOME (
    set ZOOCFGDIR=%ZOOKEEPER_HOME%\conf
  ))
)

if "%ZOOCFGDIR%" NEQ "" (
  set SQOOP_CLASSPATH=!SQOOP_CLASSPATH!;%ZOOCFGDIR%
)

call :add_dir_to_classpath %SQOOP_CONF_DIR%

:: If there's a build subdir, use Ivy-retrieved dependencies too.
if exist "%SQOOP_HOME%\build\ivy\lib\sqoop" (
  call :add_dir_to_classpath %SQOOP_HOME%\build\ivy\lib\sqoop
)

set HADOOP_CLASSPATH=%SQOOP_CLASSPATH%;%HADOOP_CLASSPATH%
if defined SQOOP_USER_CLASSPATH (
  :: User has elements to prepend to the classpath, forcibly overriding
  :: Sqoop's own lib directories.
  set HADOOP_CLASSPATH=%SQOOP_USER_CLASSPATH%;%HADOOP_CLASSPATH%
)

goto :eof

:: Function to add the given directory to the list of classpath directories
:: All jars under the given directory are added to the classpath
:add_dir_to_classpath
if not "%1"=="" (
  set SQOOP_CLASSPATH=!SQOOP_CLASSPATH!;%1\*
)
goto :eof

