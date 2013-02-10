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
Command Line Client
===================

Sqoop 2 provides command line client that is capable of communicating with Sqoop 2 server using REST interface. Client is able to run in two modes - interactive and batch mode. Commands ``create``, ``update`` and ``clone`` are not currently supported in batch mode. Interactive mode supports all available commands.

You can start Sqoop 2 client in interactive mode using provided ``sqoop.sh`` shell script by passing argument ``client``: ::

  sqoop.sh client

Batch mode can be started by adding additional argument representing path to your Sqoop client script: ::

  sqoop.sh client /path/to/your/script.sqoop

Sqoop client script is expected to contain valid Sqoop client commands, empty lines and lines starting with ``#`` that are denoting comment lines. Comments and empty lines are ignored, all other lines are interpreted. Example script: ::

  # Specify company server
  set server --host sqoop2.company.net

  # Executing given job
  submission start --jid 1

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

Example: ::

  set server --host sqoop2.company.net --port 80 --webapp sqoop

Set Option Function
~~~~~~~~~~~~~~~~~~~

Configure Sqoop client related options. This function have two required arguments ``name`` and ``value``. Name represents internal property name and value holds new value that should be set. List of available option names follows:

+-------------------+---------------+---------------------------------------------------------------------+
| Option name       | Default value | Description                                                         |
+===================+===============+=====================================================================+
| ``verbose``       | false         | Client will print additional information if verbose mode is enabled |
+-------------------+---------------+---------------------------------------------------------------------+

Example: ::

  set option --name verbose --value true

Show Command
------------

Show commands displays various information including server and protocol versions or all stored meta data.

Available functions:

+----------------+--------------------------------------------------------------------------------------------------------+
| Function       | Description                                                                                            |
+================+========================================================================================================+
| ``server``     | Display connection information to the server (host, port, webapp)                                      |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``option``     | Display various client side options                                                                    |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``version``    | Show version of both client and server (build numbers, supported protocols)                            |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``connector``  | Show connector meta data - set of parameters that connectors needs to create connections and jobs      |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``framework``  | Show framework meta data - set of parameters that Sqoop framework needs to create connections and jobs |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``connection`` | Show created connection meta data objects                                                              |
+----------------+--------------------------------------------------------------------------------------------------------+
| ``job``        | Show created job meta data objects                                                                     |
+----------------+--------------------------------------------------------------------------------------------------------+

Show Server Function
~~~~~~~~~~~~~~~~~~~~

Show details about configuration connection to Sqoop server.

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

Show versions of both client and server as well as supported protocols.

+------------------------+-----------------------------------------------+
| Argument               |  Description                                  |
+========================+===============================================+
| ``-a``, ``--all``      | Show all versions (server, client, protocols) |
+------------------------+-----------------------------------------------+
| ``-c``, ``--client``   | Show client version                           |
+------------------------+-----------------------------------------------+
| ``-s``, ``--server``   | Show server version                           |
+------------------------+-----------------------------------------------+
| ``-p``, ``--protocol`` | Show protocol support on client or server     |
+------------------------+-----------------------------------------------+

Example: ::

  show version --all

Show Connector Function
~~~~~~~~~~~~~~~~~~~~~~~

Show connector meta data - parameters that connectors need in order to create new connection and job objects.

+-----------------------+------------------------------------------------+
| Argument              |  Description                                   |
+=======================+================================================+
| ``-a``, ``--all``     | Show information for all connectors            |
+-----------------------+------------------------------------------------+
| ``-c``, ``--cid <x>`` | Show information for connector with id ``<x>`` |
+-----------------------+------------------------------------------------+

Example: ::

  show connector --all

Show Framework Function
~~~~~~~~~~~~~~~~~~~~~~~

Show framework meta data - parameters that Sqoop framework need in order to create new connection and job objects.

This function do not have any extra arguments.

Example: ::

  show framework

Show Connection Function
~~~~~~~~~~~~~~~~~~~~~~~~

Show persisted connection objects.

+-----------------------+------------------------------------------------------+
| Argument              |  Description                                         |
+=======================+======================================================+
| ``-a``, ``--all``     | Show all available connections from all connectors   |
+-----------------------+------------------------------------------------------+
| ``-x``, ``--xid <x>`` | Show connection with id ``<x>``                      |
+-----------------------+------------------------------------------------------+

Example: ::

  show connection --all

Show Job Function
~~~~~~~~~~~~~~~~~

Show persisted job objects.

+-----------------------+----------------------------------------------+
| Argument              |  Description                                 |
+=======================+==============================================+
| ``-a``, ``--all``     | Show all available jobs from all connectors  |
+-----------------------+----------------------------------------------+
| ``-j``, ``--jid <x>`` | Show job with id ``<x>``                     |
+-----------------------+----------------------------------------------+

Example: ::

  show job --all

Create Command
--------------

Creates new connection and job objects. This command is supported only in interactive mode. It will query user for all parameters that are required by specific connector and framework and persist them in Sqoop server for later use.

Available functions:

+----------------+-------------------------------------------------+
| Function       | Description                                     |
+================+=================================================+
| ``connection`` | Create new connection object                    |
+----------------+-------------------------------------------------+
| ``job``        | Create new job object                           |
+----------------+-------------------------------------------------+

Create Connection Function
~~~~~~~~~~~~~~~~~~~~~~~~~~

Create new connection object.

+------------------------+-------------------------------------------------------------+
| Argument               |  Description                                                |
+========================+=============================================================+
| ``-c``, ``--cid <x>``  |  Create new connection object for connector with id ``<x>`` |
+------------------------+-------------------------------------------------------------+


Example: ::

  create connection --cid 1

Create Job Function
~~~~~~~~~~~~~~~~~~~

Create new job object.

+------------------------+------------------------------------------------------------------+
| Argument               |  Description                                                     |
+========================+==================================================================+
| ``-x``, ``--xid <x>``  | Create new job object for connection with id ``<x>``             |
+------------------------+------------------------------------------------------------------+
| ``-t``, ``--type <t>`` | Create new job object with type ``<t>`` (``import``, ``export``) |
+------------------------+------------------------------------------------------------------+

Example: ::

  create job --xid 1

Update Command
--------------

Update commands allows you to edit connection and job objects - change persisted meta data. This command is supported only in interactive mode.

Update Connection Function
~~~~~~~~~~~~~~~~~~~~~~~~~~

Update existing connection object.

+-----------------------+---------------------------------------------+
| Argument              |  Description                                |
+=======================+=============================================+
| ``-x``, ``--xid <x>`` |  Update existing connection with id ``<x>`` |
+-----------------------+---------------------------------------------+

Example: ::

  update connection --xid 1

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

Deletes connection and job objects from Sqoop server.

Delete Connection Function
~~~~~~~~~~~~~~~~~~~~~~~~~~

Delete existing connection object.

+-----------------------+-------------------------------------------+
| Argument              |  Description                              |
+=======================+===========================================+
| ``-x``, ``--xid <x>`` |  Delete connection object with id ``<x>`` |
+-----------------------+-------------------------------------------+

Example: ::

  delete connection --xid 1


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

Clone command will load existing connection or job object from Sqoop server and allow user in place changes that will result in creation of new connection or job object. This command is not supported in batch mode.

Clone Connection Function
~~~~~~~~~~~~~~~~~~~~~~~~~

Clone existing connection object.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-x``, ``--xid <x>`` |  Clone connection object with id ``<x>`` |
+-----------------------+------------------------------------------+

Example: ::

  clone connection --xid 1


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


Submission Command
------------------

Submission command is entry point for executing actual data transfers. It allows you to start, stop and retrieve status of currently running jobs.

Available functions:

+----------------+-------------------------------------------------+
| Function       | Description                                     |
+================+=================================================+
| ``start``      | Start job                                       |
+----------------+-------------------------------------------------+
| ``stop``       | Interrupt running job                           |
+----------------+-------------------------------------------------+
| ``status``     | Retrieve status for given job                   |
+----------------+-------------------------------------------------+

Submission Start Function
~~~~~~~~~~~~~~~~~~~~~~~~~

Start job (submit new submission). Starting already running job is considered as invalid operation.

+-----------------------+---------------------------+
| Argument              |  Description              |
+=======================+===========================+
| ``-j``, ``--jid <x>`` | Start job with id ``<x>`` |
+-----------------------+---------------------------+

Example: ::

  submission start --jid 1


Submission Stop Function
~~~~~~~~~~~~~~~~~~~~~~~~~

Interrupt running job.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Interrupt running job with id ``<x>``    |
+-----------------------+------------------------------------------+

Example: ::

  submission stop --jid 1

Submission Status Function
~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve last status for given job.

+-----------------------+------------------------------------------+
| Argument              |  Description                             |
+=======================+==========================================+
| ``-j``, ``--jid <x>`` | Retrieve status for job with id ``<x>``  |
+-----------------------+------------------------------------------+

Example: ::

  submission status --jid 1

