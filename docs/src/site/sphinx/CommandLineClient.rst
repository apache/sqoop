.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


===================
Command Line Shell
===================

Sqoop 2 provides command line shell that is capable of communicating with Sqoop 2 server using REST interface. Client is able to run in two modes - interactive and batch mode. Commands ``create``, ``update`` and ``clone`` are not currently supported in batch mode. Interactive mode supports all available commands.

You can start Sqoop 2 client in interactive mode using command ``sqoop2-shell``::

  sqoop2-shell

Batch mode can be started by adding additional argument representing path to your Sqoop client script: ::

  sqoop2-shell /path/to/your/script.sqoop

Sqoop client script is expected to contain valid Sqoop client commands, empty lines and lines starting with ``#`` that are denoting comment lines. Comments and empty lines are ignored, all other lines are interpreted. Example script: ::

  # Specify company server
  set server --host sqoop2.company.net

  # Executing given job
  start job  --jid 1


.. contents:: Table of Contents

Resource file
=============

Sqoop 2 client have ability to load resource files similarly as other command line tools. At the beginning of execution Sqoop client will check existence of file ``.sqoop2rc`` in home directory of currently logged user. If such file exists, it will be interpreted before any additional actions. This file is loaded in both interactive and batch mode. It can be used to execute any batch compatible commands.

Example resource file: ::

  # Configure our Sqoop 2 server automatically
  set server --host sqoop2.company.net

  # Run in verbose mode by default
  set option --name verbose --value true

Commands
========

Sqoop 2 contains several commands that will be documented in this section. Each command have one more functions that are accepting various arguments. Not all commands are supported in both interactive and batch mode.

Auxiliary Commands
------------------

Auxiliary commands are commands that are improving user experience and are running purely on client side. Thus they do not need working connection to the server.

* ``exit`` Exit client immediately. This command can be also executed by sending EOT (end of transmission) character. It's CTRL+D on most common Linux shells like Bash or Zsh.
* ``history`` Print out command history. Please note that Sqoop client is saving history from previous executions and thus you might see commands that you've executed in previous runs.
* ``help`` Show all available commands with short in-shell documentation.

::

 sqoop:000> help
 For information about Sqoop, visit: http://sqoop.apache.org/

 Available commands:
   exit    (\x  ) Exit the shell
   history (\H  ) Display, manage and recall edit-line history
   help    (\h  ) Display this help message
   set     (\st ) Configure various client options and settings
   show    (\sh ) Display various objects and configuration options
   create  (\cr ) Create new object in Sqoop repository
   delete  (\d  ) Delete existing object in Sqoop repository
   update  (\up ) Update objects in Sqoop repository
   clone   (\cl ) Create new object based on existing one
   start   (\sta) Start job
   stop    (\stp) Stop job
   status  (\stu) Display status of a job
   enable  (\en ) Enable object in Sqoop repository
   disable (\di ) Disable object in Sqoop repository

Set Command
-----------

Set command allows to set various properties of the client. Similarly as auxiliary commands, set do not require connection to Sqoop server. Set commands is not used to reconfigure Sqoop server.

Available functions:

+---------------+------------------------------------------+
| Function      | Description                              |
+===============+==========================================+
| ``server``    | Set connection configuration for server  |
+---------------+------------------------------------------+
| ``option``    | Set various client side options          |
+---------------+------------------------------------------+

Set Server Function
~~~~~~~~~~~~~~~~~~~

Configure connection to Sqoop server - host port and web application name. Available arguments:

+-----------------------+---------------+--------------------------------------------------+
| Argument              | Default value | Description                                      |
+=======================+===============+==================================================+
| ``-h``, ``--host``    | localhost     | Server name (FQDN) where Sqoop server is running |
+-----------------------+---------------+--------------------------------------------------+
| ``-p``, ``--port``    | 12000         | TCP Port                                         |
+-----------------------+---------------+--------------------------------------------------+
| ``-w``, ``--webapp``  | sqoop         | Tomcat's web application name                    |
+-----------------------+---------------+--------------------------------------------------+
| ``-u``, ``--url``     |               | Sqoop Server in url format                       |
+-----------------------+---------------+--------------------------------------------------+

Example: ::

  set server --host sqoop2.company.net --port 80 --webapp sqoop

or ::

  set server --url http://sqoop2.company.net:80/sqoop

Note: When ``--url`` option is given, ``--host``, ``--port`` or ``--webapp`` option will be ignored.

Set Option Function
~~~~~~~~~~~~~~~~~~~

Configure Sqoop client related options. This function have two required arguments ``name`` and ``value``. Name represents internal property name and value holds new value that should be set. List of available option names follows:

+-------------------+---------------+---------------------------------------------------------------------+
| Option name       | Default value | Description                                                         |
+===================+===============+=====================================================================+
| ``verbose``       | false         | Client will print additional information if verbose mode is enabled |
+-------------------+---------------+---------------------------------------------------------------------+
| ``poll-timeout``  | 10000         | Server poll timeout in milliseconds                                 |
+-------------------+---------------+---------------------------------------------------------------------+

Example: ::

  set option --name verbose --value true
  set option --name poll-timeout --value 20000

Show Command
------------

Show commands displays various information as described below.

Available functions:

+----------------+--------------------------------------------------------------------------------------------------------+
| Function       | Description                                                                                            |
+================+========================================================================================================+
| ``server``     | Display connection information to the sqoop server (host, port, webapp)                                |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``option``     | Display various client side options                                                                    |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``version``    | Show client build version, with an option -all it shows server build version and supported api versions|
+----------------+--------------------------------------------------------------------------------------------------------+
| ``connector``  | Show connector configurable and its related configs                                                    |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``driver``     | Show driver configurable and its related configs                                                       |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``link``       | Show links in sqoop                                                                                    |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``job``        | Show jobs in sqoop                                                                                     |
+----------------+--------------------------------------------------------------------------------------------------------+

Show Server Function
~~~~~~~~~~~~~~~~~~~~

Show details about connection to Sqoop server.

+-----------------------+--------------------------------------------------------------+
| Argument              |  Description                                                 |
+=======================+==============================================================+
| ``-a``, ``--all``     | Show all connection related information (host, port, webapp) |
+-----------------------+--------------------------------------------------------------+
| ``-h``, ``--host``    | Show host                                                    |
+-----------------------+--------------------------------------------------------------+
| ``-p``, ``--port``    | Show port                                                    |
+-----------------------+--------------------------------------------------------------+
| ``-w``, ``--webapp``  | Show web application name                                    |
+-----------------------+--------------------------------------------------------------+

Example: ::

  show server --all

Show Option Function
~~~~~~~~~~~~~~~~~~~~

Show values of various client side options. This function will show all client options when called without arguments.

+-----------------------+--------------------------------------------------------------+
| Argument              |  Description                                                 |
+=======================+==============================================================+
| ``-n``, ``--name``    | Show client option value with given name                     |
+-----------------------+--------------------------------------------------------------+

Please check table in `Set Option Function`_ section to get a list of all supported option names.

Example: ::

  show option --name verbose

Show Version Function
~~~~~~~~~~~~~~~~~~~~~

Show build versions of both client and server as well as the supported rest api versions.

+------------------------+-----------------------------------------------+
| Argument               |  Description                                  |
+========================+===============================================+
| ``-a``, ``--all``      | Show all versions (server, client, api)       |
+------------------------+-----------------------------------------------+
| ``-c``, ``--client``   | Show client build version                     |
+------------------------+-----------------------------------------------+
| ``-s``, ``--server``   | Show server build version                     |
+------------------------+-----------------------------------------------+
| ``-p``, ``--api``      | Show supported api versions                   |
+------------------------+-----------------------------------------------+

Example: ::

  show version --all

Show Connector Function
~~~~~~~~~~~~~~~~~~~~~~~

Show persisted connector configurable and its related configs used in creating associated link and job objects

+-----------------------+------------------------------------------------+
| Argument              |  Description                                   |
+=======================+================================================+
| ``-a``, ``--all``     | Show information for all connectors            |
+-----------------------+------------------------------------------------+
| ``-c``, ``--cid <x>`` | Show information for connector with id ``<x>`` |
+-----------------------+------------------------------------------------+

Example: ::

  show connector --all or show connector

Show Driver Function
~~~~~~~~~~~~~~~~~~~~

Show persisted driver configurable and its related configs used in creating job objects

This function do not have any extra arguments. There is only one registered driver in sqoop

Example: ::

  show driver

Show Link Function
~~~~~~~~~~~~~~~~~~

Show persisted link objects.

+-----------------------+------------------------------------------------------+
| Argument              |  Description                                         |
+=======================+======================================================+
| ``-a``, ``--all``     | Show all available links                             |
+-----------------------+------------------------------------------------------+
| ``-x``, ``--lid <x>`` | Show link with id ``<x>``                            |
+-----------------------+------------------------------------------------------+

Example: ::

  show link --all or show link

Show Job Function
~~~~~~~~~~~~~~~~~

Show persisted job objects.

+-----------------------+----------------------------------------------+
| Argument              |  Description                                 |
+=======================+==============================================+
| ``-a``, ``--all``     | Show all available jobs                      |
+-----------------------+----------------------------------------------+
| ``-j``, ``--jid <x>`` | Show job with id ``<x>``                     |
+-----------------------+----------------------------------------------+

Example: ::

  show job --all or show job

Show Submission Function
~~~~~~~~~~~~~~~~~~~~~~~~

Show persisted job submission objects.

+-----------------------+---------------------------------------------+
| Argument              |  Description                                |
+=======================+=============================================+
| ``-j``, ``--jid <x>`` | Show available submissions for given job    |
+-----------------------+---------------------------------------------+
| ``-d``, ``--detail``  | Show job submissions in full details        |
+-----------------------+---------------------------------------------+

Example: ::

  show submission
  show submission --jid 1
  show submission --jid 1 --detail

Create Command
--------------

Creates new link and job objects. This command is supported only in interactive mode. It will ask user to enter the link config and job configs for from /to and driver when creating link and job objects respectively.

Available functions:

+----------------+-------------------------------------------------+
| Function       | Description                                     |
+================+=================================================+
| ``link``       | Create new link object                          |
+----------------+-------------------------------------------------+
| ``job``        | Create new job object                           |
+----------------+-------------------------------------------------+

Create Link Function
~~~~~~~~~~~~~~~~~~~~

Create new link object.

+------------------------+-------------------------------------------------------------+
| Argument               |  Description                                                |
+========================+=============================================================+
| ``-c``, ``--cid <x>``  |  Create new link object for connector with id ``<x>``       |
+------------------------+-------------------------------------------------------------+


Example: ::

  create link --cid 1 or create link -c 1

Create Job Function
~~~~~~~~~~~~~~~~~~~

Create new job object.

+------------------------+------------------------------------------------------------------+
| Argument               |  Description                                                     |
+========================+==================================================================+
| ``-f``, ``--from <x>`` | Create new job object with a FROM link with id ``<x>``           |
+------------------------+------------------------------------------------------------------+
| ``-t``, ``--to <t>``   | Create new job object with a TO link with id ``<x>``             |
+------------------------+------------------------------------------------------------------+

Example: ::

  create job --from 1 --to 2 or create job --f 1 --t 2 

Update Command
--------------

Update commands allows you to edit link and job objects. This command is supported only in interactive mode.

Update Link Function
~~~~~~~~~~~~~~~~~~~~

Update existing link object.

+-----------------------+---------------------------------------------+
| Argument              |  Description                                |
+=======================+=============================================+
| ``-x``, ``--lid <x>`` |  Update existing link with id ``<x>``       |
+-----------------------+---------------------------------------------+

Example: ::

  update link --lid 1

Update Job Function
~~~~~~~~~~~~~~~~~~~

Update existing job object.

+-----------------------+--------------------------------------------+
| Argument              |  Description                               |
+=======================+============================================+
| ``-j``, ``--jid <x>`` | Update existing job object with id ``<x>`` |
+-----------------------+--------------------------------------------+

Example: ::

  update job --jid 1


Delete Command
--------------

Deletes link and job objects from Sqoop server.

Delete Link Function
~~~~~~~~~~~~~~~~~~~~

Delete existing link object.

+-----------------------+-------------------------------------------+
| Argument              |  Description                              |
+=======================+===========================================+
| ``-x``, ``--lid <x>`` |  Delete link object with id ``<x>``       |
+-----------------------+-------------------------------------------+

Example: ::

  delete link --lid 1


Delete Job Function
~~~~~~~~~~~~~~~~~~~

Delete existing job object.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Delete job object with id ``<x>``        |
+-----------------------+------------------------------------------+

Example: ::

  delete job --jid 1


Clone Command
-------------

Clone command will load existing link or job object from Sqoop server and allow user in place updates that will result in creation of new link or job object. This command is not supported in batch mode.

Clone Link Function
~~~~~~~~~~~~~~~~~~~~~~~~~

Clone existing link object.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-x``, ``--lid <x>`` |  Clone link object with id ``<x>``       |
+-----------------------+------------------------------------------+

Example: ::

  clone link --lid 1


Clone Job Function
~~~~~~~~~~~~~~~~~~

Clone existing job object.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Clone job object with id ``<x>``         |
+-----------------------+------------------------------------------+

Example: ::

  clone job --jid 1

Start Command
-------------

Start command will begin execution of an existing Sqoop job.

Start Job Function
~~~~~~~~~~~~~~~~~~

Start job (submit new submission). Starting already running job is considered as invalid operation.

+----------------------------+----------------------------+
| Argument                   |  Description               |
+============================+============================+
| ``-j``, ``--jid <x>``      | Start job with id ``<x>``  |
+----------------------------+----------------------------+
| ``-s``, ``--synchronous``  | Synchoronous job execution |
+----------------------------+----------------------------+

Example: ::

  start job --jid 1
  start job --jid 1 --synchronous

Stop Command
------------

Stop command will interrupt an job execution.

Stop Job Function
~~~~~~~~~~~~~~~~~

Interrupt running job.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Interrupt running job with id ``<x>``    |
+-----------------------+------------------------------------------+

Example: ::

  stop job --jid 1

Status Command
--------------

Status command will retrieve the last status of a job.

Status Job Function
~~~~~~~~~~~~~~~~~~~

Retrieve last status for given job.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Retrieve status for job with id ``<x>``  |
+-----------------------+------------------------------------------+

Example: ::

  status job --jid 1