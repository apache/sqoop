.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF lANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


=====
Tools
=====

Tools are server commands that administrators can execute on the Sqoop server machine in order to perform various maintenance tasks. The tool execution will always perform a given task and finish. There are no long running services implemented as tools.

In order to perform the maintenance task each tool is suppose to do, they need to be executed in exactly the same environment as the main Sqoop server. The tool binary will take care of setting up the ``CLASSPATH`` and other environmental variables that might be required. However it's up to the administrator himself to run the tool under the same user as is used for the server. This is usually configured automatically for various Hadoop distributions (such as Apache Bigtop).


.. note:: Running tools under a different user such as ``root`` might prevent Sqoop Server from running correctly.

List of available tools:

* verify
* upgrade

To run the desired tool, execute binary ``sqoop2-tool`` with desired tool name. For example to run ``verify`` tool::

  sqoop2-tool verify

.. note:: Running tools while the Sqoop Server is also running is not recommended as it might lead to a data corruption and service disruption.

Verify
======

The verify tool will verify Sqoop server configuration by starting all subsystems with the exception of servlets and tearing them down.

To run the ``verify`` tool::

  sqoop2-tool verify

If the verification process succeeds, you should see messages like::

  Verification was successful.
  Tool class org.apache.sqoop.tools.tool.VerifyTool has finished correctly

If the verification process will find any inconsistencies, it will print out the following message instead::

  Verification has failed, please check Server logs for further details.
  Tool class org.apache.sqoop.tools.tool.VerifyTool has failed.

Further details why the verification has failed will be available in the Sqoop server log - same file as the Sqoop Server logs into.

Upgrade
=======

Upgrades all versionable components inside Sqoop2. This includes structural changes inside the repository and stored metadata. Running this tool is idempotent.

To run the ``upgrade`` tool::

  sqoop2-tool upgrade

Upon successful upgrade you should see following message::

  Tool class org.apache.sqoop.tools.tool.UpgradeTool has finished correctly.

Execution failure will show the following message instead::

  Tool class org.apache.sqoop.tools.tool.UpgradeTool has failed.

Further details why the upgrade process has failed will be available in the Sqoop server log - same file as the Sqoop Server logs into.
