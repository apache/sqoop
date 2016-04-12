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


.. note:: Running tools while the Sqoop Server is also running is not recommended as it might lead to a data corruption and service disruption.

List of available tools:

* verify
* upgrade

To run the desired tool, execute binary ``sqoop2-tool`` with desired tool name. For example to run ``verify`` tool::

  sqoop2-tool verify

.. note:: Stop the Sqoop Server before running Sqoop tools. Running tools while Sqoop Server is running can lead to a data corruption and service disruption.

.. _tool-verify:

Verify tool
===========

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

.. _tool-upgrade:

Upgrade tool
============

Upgrades all versionable components inside Sqoop2. This includes structural changes inside the repository and stored metadata.
Running this tool on Sqoop deployment that was already upgraded will have no effect.

To run the ``upgrade`` tool::

  sqoop2-tool upgrade

Upon successful upgrade you should see following message::

  Tool class org.apache.sqoop.tools.tool.UpgradeTool has finished correctly.

Execution failure will show the following message instead::

  Tool class org.apache.sqoop.tools.tool.UpgradeTool has failed.

Further details why the upgrade process has failed will be available in the Sqoop server log - same file as the Sqoop Server logs into.

RepositoryDump
==============

Writes the user-created contents of the Sqoop repository to a file in JSON format. This includes connections, jobs and submissions.

To run the ``repositorydump`` tool::

  sqoop2-tool repositorydump -o repository.json

As an option, the administrator can choose to include sensitive information such as database connection passwords in the file::

  sqoop2-tool repositorydump -o repository.json --include-sensitive

Upon successful execution, you should see the following message::

  Tool class org.apache.sqoop.tools.tool.RepositoryDumpTool has finished correctly.

If repository dump has failed, you will see the following message instead::

  Tool class org.apache.sqoop.tools.tool.RepositoryDumpTool has failed.

Further details why the upgrade process has failed will be available in the Sqoop server log - same file as the Sqoop Server logs into.

RepositoryLoad
==============

Reads a json formatted file created by RepositoryDump and loads to current Sqoop repository.

To run the ``repositoryLoad`` tool::

  sqoop2-tool repositoryload -i repository.json

Upon successful execution, you should see the following message::

  Tool class org.apache.sqoop.tools.tool.RepositoryLoadTool has finished correctly.

If repository load failed you will see the following message instead::

 Tool class org.apache.sqoop.tools.tool.RepositoryLoadTool has failed.

Or an exception. Further details why the upgrade process has failed will be available in the Sqoop server log - same file as the Sqoop Server logs into.

.. note:: If the repository dump was created without passwords (default), the connections will not contain a password and the jobs will fail to execute. In that case you'll need to manually update the connections and set the password.
.. note:: RepositoryLoad tool will always generate new connections, jobs and submissions from the file. Even when an identical objects already exists in repository.

.. _repositoryencryption-tool:

RepositoryEncryption
====================

Please see :ref:`repositoryencryption` for more details on repository encryption.

Sometimes we may want to change the password that is used to encrypt our data, generate a new key for our existing password,
encrypt an existing unencrypted repository, or decrypt an existing encrypting repository. Sqoop 2 provides the
Repository Encryption Tool to allow us to do this.

Before using the tool it is important to shut down the Sqoop 2 server.

All changes that the tool makes occur in a single transaction with the repository, which will prevent leaving the
repository in a bad state.

The Repository Encryption Tool is very simple, it uses the exact same configuration specified above (with the exception
of ``useConf``). Configuration prefixed with a "-F" represents the existing repository state, configuration prefixed with
a "-T" represents the desired repository state. If one of these configuration sets is left out that means unencrypted.

Changing the Password
---------------------

In order to change the password, we need to specify the current configuration with the existing password and the desired
configuration with the new password. It looks like this:

::

    sqoop.sh tool repositoryencryption \
        -Forg.apache.sqoop.security.repo_encryption.password=old_password \
        -Forg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Forg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Forg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000 \
        -Torg.apache.sqoop.security.repo_encryption.password=new_password \
        -Torg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Torg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Torg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000

Generate a New Key for the Existing Password
--------------------------------------------

Just like with the previous scenario you could copy the same configuration twice like this:

::

    sqoop.sh tool repositoryencryption \
        -Forg.apache.sqoop.security.repo_encryption.password=password \
        -Forg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Forg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Forg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000 \
        -Torg.apache.sqoop.security.repo_encryption.password=password \
        -Torg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Torg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Torg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000

But we do have a shortcut to make this easier:

::

    sqoop.sh tool repositoryencryption -FuseConf -TuseConf

The ``useConf`` option will read whatever configuration is already in the configured sqoop properties file and apply it
for the specified direction.

Encrypting an Existing Unencrypted Repository
---------------------------------------------

::

    sqoop.sh tool repositoryencryption \
        -Torg.apache.sqoop.security.repo_encryption.password=password \
        -Torg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Torg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Torg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Torg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000

If the configuration for the encrypted repository has already been written to the sqoop properties file, one can simply
execute:

::

    sqoop.sh tool repositoryencryption -TuseConf


Decrypting an Existing Encrypted Repository
-------------------------------------------

::

    sqoop.sh tool repositoryencryption \
        -Forg.apache.sqoop.security.repo_encryption.password=password \
        -Forg.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_algorithm=AES \
        -Forg.apache.sqoop.security.repo_encryption.cipher_key_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding \
        -Forg.apache.sqoop.security.repo_encryption.initialization_vector_size=16 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1 \
        -Forg.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000

If the configuration for the encrypted repository has not yet been removed from the sqoop properties file, one can simply
execute:

::

    sqoop.sh tool repositoryencryption -FuseConf
