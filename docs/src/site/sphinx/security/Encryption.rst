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

=====================
Repository Encryption
=====================

Sqoop 2 uses a database to store metadata about the various data sources it talks to, we call this database the repository.

The repository can store passwords and other pieces of information that are security sensitive, within the context of Sqoop
2, this information is referred to as sensitive inputs. Which inputs are considered sensitive is determined by the connector.

We support encrypting sensitive inputs in the repository using a provided password or password generator. Sqoop 2 uses the
provided password and the provided key generation algorithm (such as PBKDF2) to generate a key to encrypt sensitive inputs
and another hmac key to verify their integrity.

Only the sensitive inputs are encrypted. If an input is not defined as sensitive by the connector, it is NOT encrypted.

Server Configuration
=====================

Note: This configuration will allow a new Sqoop instance to encrypt information or read from an already encrypted repository.
It will not encrypt sensitive inputs in an existing repository. Please see below for instructions on how to encrypt an existing repository.

First, repository encryption must be enabled.
::

    org.apache.sqoop.security.repo_encryption.enabled=true

Then we configure the password:

::

    org.apache.sqoop.security.repo_encryption.password=supersecret

Or the password generator:

::

    org.apache.sqoop.security.repo_encryption.password_generator=echo supersecret

The plaintext password is always given preference to the password generator if both are present.

Then we can configure the HMAC algorithm. Please find the list of possibilities here:
`Standard Algorithm Name Documentation - Mac <http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#Mac>`_
We can store digests with up to 1024 bits.

::

    org.apache.sqoop.security.repo_encryption.hmac_algorithm=HmacSHA256

Then we configure the cipher algorithm. Possibilities can be found here:
`Standard Algorithm Name Documentation - Cipher <http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#Cipher>`_

::

    org.apache.sqoop.security.repo_encryption.cipher_algorithm=AES

Then we configure the key size for the cipher in bytes. We can store up to 1024 bit keys.

::

    org.apache.sqoop.security.repo_encryption.cipher_key_size=16

Next we need to specify the cipher transformation. The options for this field are listed here:
`Cipher (Java Platform SE 7) <http://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html>`_

::

    org.apache.sqoop.security.repo_encryption.cipher_spec=AES/CBC/PKCS5Padding

The size of the initialization vector to use in bytes. We support up to 1024 bit initialization vectors.

::

    org.apache.sqoop.security.repo_encryption.initialization_vector_size=16

Next we need to specfy the algorithm for secret key generation. Please refer to:
`Standard Algorithm Name Documentation - SecretKeyFactory <http://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SecretKeyFactory>`_

::

    org.apache.sqoop.security.repo_encryption.pbkdf2_algorithm=PBKDF2WithHmacSHA1

Finally specify the number of rounds/iterations for the generation of a key from a password.

::

    org.apache.sqoop.security.repo_encryption.pbkdf2_rounds=4000

Repository Encryption Tool
==========================

Sometimes we may want to change the password that is used to encrypt our data, generate a new key for our existing password,
encrypt an existing unencrypted repository, or decrypt an existing encrypting repository. Sqoop 2 provides the
Repository Encryption Tool to allow us to do this.

Before using the tool it is important to shut down the Sqoop 2 server.

All changes that the tool makes occur in a single transaction with the repository, which should prevent leaving the
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
