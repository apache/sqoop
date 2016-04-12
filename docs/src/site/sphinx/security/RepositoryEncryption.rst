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

.. _repositoryencryption:

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
It will not encrypt sensitive inputs in an existing repository. For instructions on how to encrypt an existing repository,
please look here: :ref:`repositoryencryption-tool`

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
