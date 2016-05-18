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


.. _apitlsssl:

===========
API TLS/SSL
===========

Sqoop 2 offers an HTTP REST-like API as the mechanism by which clients can
communicate with the Sqoop 2 server. The Sqoop 2 server and the Sqoop 2 shell
have support for TLS/SSL.

Keystore Generation
===================

Sqoop 2 uses the JKS format. Details on how to create JKS files can be found here:
`Generating a KeyStore and TrustStore <https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html>`_

Server Configuration
=====================

All Sqoop 2 server TLS/SSL configuration occurs in the Sqoop configuration file,
normally in ``<Sqoop Folder>/conf/sqoop.properties``.

First, TLS must be enabled:

::

   org.apache.sqoop.security.tls.enabled=true

A protocol should be specified. Please find a list of options here:
`Standard Algorithm Name Documentation <http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext>`_

::

   org.apache.sqoop.security.tls.protocol="TLSv1.2"


Configure the path to the JKS keystore:

::

   org.apache.sqoop.security.tls.keystore=/Users/abe/mykeystore.jks

Configure the keystore password and the key manager password:

::

   org.apache.sqoop.security.tls.keystore_password=keystorepassword
   org.apache.sqoop.security.tls.keymanager_password=keymanagerpassword

Alternatively, the password can be specified using generators.

Generators are commands that the Sqoop propess will execute, and then retrieve the
password from standard out. The generator will only be run if no standard password
is configured.

::

   org.apache.sqoop.security.tls.keystore_password_generator=echo keystorepassword
   org.apache.sqoop.security.tls.keymanager_password=echo keymanagerpassword

Client/Shell Configuration
==========================

When using TLS on the Sqoop 2 server, especially with a self-signed certificate,
it may be useful to specify a truststore for the client/shell to use.

The truststore for the shell is configured via a command. In practice, it may be
useful to put this command inside the system sqoop rc file (``/etc/sqoop2/conf/sqoop2rc``)
or the user's rc file (``~/.sqoop2rc``).

::

   sqoop:000> set truststore --truststore /Users/abefine/keystore/node2.truststore
   Truststore set successfully

You may also include a password. Passwords are not required for truststores.

::

   sqoop:000> set truststore --truststore /Users/abefine/keystore/node2.truststore --truststore-password changeme
   Truststore set successfully

You may also use a password generator.

::

   sqoop:000> set truststore --truststore /Users/abefine/keystore/node2.truststore --truststore-password-generator "echo changeme"
   Truststore set successfully
