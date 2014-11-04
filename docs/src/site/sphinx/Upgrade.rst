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


=======
Upgrade
=======

This page describes procedure that you need to take in order to upgrade Sqoop from one release to a higher release. Upgrading both client and server component will be discussed separately.

.. note:: Only updates from one Sqoop 2 release to another are covered, starting with upgrades from version 1.99.2. This guide do not contain general information how to upgrade from Sqoop 1 to Sqoop 2.

Upgrading Server
================

As Sqoop server is using a database repository for persisting sqoop entities such as the connector, driver, links and jobs the repository schema might need to be updated as part of the server upgrade. In addition the configs and inputs described by the various connectors and the driver may also change with a new server version and might need a data upgrade.

There are two ways how to upgrade Sqoop entities in the repository, you can either execute upgrade tool or configure the sqoop server to perform all necessary upgrades on start up.

It's strongly advised to back up the repository before moving on to next steps. Backup instructions will vary depending on the repository implementation. For example, using MySQL as a repository will require a different back procedure than Apache Derby. Please follow the repositories' backup procedure.

Upgrading Server using upgrade tool
-----------------------------------

Preferred upgrade path is to explicitly run the `Upgrade Tool <Tools.html#upgrade>`_. First step is to however shutdown the server as having both the server and upgrade utility accessing the same repository might corrupt it::

  sqoop2-server stop

When the server has been successfully stopped, you can update the server bits and simply run the upgrade tool::

  sqoop2-tool upgrade

You should see that the upgrade process has been successful::

  Tool class org.apache.sqoop.tools.tool.UpgradeTool has finished correctly.

In case of any failure, please take a look into `Upgrade Tool <Tools.html#upgrade>`_ documentation page.

Upgrading Server on start-up
----------------------------

The capability of performing the upgrade has been built-in to the server, however is disabled by default to avoid any unintentional changes to the repository. You can start the repository schema upgrade procedure by stopping the server: ::

  sqoop2-server stop

Before starting the server again you will need to enable the auto-upgrade feature that will perform all necessary changes during Sqoop Server start up.

You need to set the following property in configuration file ``sqoop.properties`` for the repository schema upgrade.
::

   org.apache.sqoop.repository.schema.immutable=false

You need to set the following property in configuration file ``sqoop.properties`` for the connector config data upgrade.
::

   org.apache.sqoop.connector.autoupgrade=true

You need to set the following property in configuration file ``sqoop.properties`` for the driver config data upgrade.
::

   org.apache.sqoop.driver.autoupgrade=true

When all properties are set, start the sqoop server using the following command::

  sqoop2-server start

All required actions will be performed automatically during the server bootstrap. It's strongly advised to set all three properties to their original values once the server has been successfully started and the upgrade has completed

Upgrading Client
================

Client do not require any manual steps during upgrade. Replacing the binaries with updated version is sufficient.
