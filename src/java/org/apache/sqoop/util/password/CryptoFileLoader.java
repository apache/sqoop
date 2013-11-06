/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.util.password;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;


/**
 * Example implementation of "advanced" file loader that will read password from
 * encrypted file. Please note that this method is merely obfuscating the password,
 * as malicious user will be able to retrieve the password if he intercepts the
 * Sqoop commands. He won't be able to get it if he will just get the password
 * file though. Current implementation is limited to ECB and is not supporting other
 * methods.
 *
 * Example usage:
 *   sqoop import  \
 *     -Dorg.apache.sqoop.credentials.loader.class=org.apache.sqoop.util.password.CryptoFileLoader \
 *     -Dorg.apache.sqoop.credentials.loader.crypto.passphrase=sqooppass \
 *     --connect ...
 */
public class CryptoFileLoader extends FilePasswordLoader {

  /**
   * Crypto algorithm that should be used.
   *
   * List of available ciphers is for example available here:
   * http://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
   */
  private static String PROPERTY_CRYPTO_ALG = "org.apache.sqoop.credentials.loader.crypto.alg";

  /**
   * Salt that should be used.
   *
   * Some algorithms are requiring salt to be present.
   */
  private static String PROPERTY_CRYPTO_SALT = "org.apache.sqoop.credentials.loader.crypto.salt";

  /**
   * Iterations argument for creating key.
   */
  private static String PROPERTY_CRYPTO_ITERATIONS = "org.apache.sqoop.credentials.loader.crypto.iterations";

  /**
   * Length of the key (driven by the used algorithm).
   */
  private static String PROPERTY_CRYPTO_KEY_LEN = "org.apache.sqoop.credentials.loader.crypto.salt.key.len";

  /**
   * Passphrase (encryption password).
   */
  private static String PROPERTY_CRYPTO_PASSPHRASE = "org.apache.sqoop.credentials.loader.crypto.passphrase";

  /**
   * Default is AES in electronic code book with padding.
   */
  private static String DEFAULT_ALG = "AES/ECB/PKCS5Padding";

  /**
   * Default salt is not much secure, use your own!
   */
  private static String DEFAULT_SALT = "SALT";

  /**
   * Iterate 10000 times by default.
   */
  private static int DEFAULT_ITERATIONS = 10000;

  /**
   * One of valid key sizes for default algorithm (AES).
   */
  private static int DEFAULT_KEY_LEN = 128;

  @Override
  public String loadPassword(String p, Configuration configuration) throws IOException {
    LOG.debug("Fetching password from specified path: " + p);
    Path path = new Path(p);
    FileSystem fs = path.getFileSystem(configuration);

    byte [] encrypted;
    try {
      verifyPath(fs, path);
      encrypted = readBytes(fs, path);
    } finally {
      fs.close();
    }

    String passPhrase = configuration.get(PROPERTY_CRYPTO_PASSPHRASE);
    if(passPhrase == null) {
      throw new IOException("Passphrase is missing in property " + PROPERTY_CRYPTO_PASSPHRASE);
    }

    String alg = configuration.get(PROPERTY_CRYPTO_ALG, DEFAULT_ALG);
    String algOnly = alg.split("/")[0];
    String salt = configuration.get(PROPERTY_CRYPTO_SALT, DEFAULT_SALT);
    int iterations = configuration.getInt(PROPERTY_CRYPTO_ITERATIONS, DEFAULT_ITERATIONS);
    int keyLen = configuration.getInt(PROPERTY_CRYPTO_KEY_LEN, DEFAULT_KEY_LEN);

    SecretKeyFactory factory = null;
    try {
      factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't load SecretKeyFactory", e);
    }

    SecretKeySpec key = null;
    try {
      key = new SecretKeySpec(factory.generateSecret(new PBEKeySpec(passPhrase.toCharArray(), salt.getBytes(), iterations, keyLen)).getEncoded(), algOnly);
    } catch (Exception e) {
      throw new IOException("Can't generate secret key", e);
    }

    Cipher crypto = null;

    try {
      crypto = Cipher.getInstance(alg);
    } catch (Exception e) {
      throw new IOException("Can't initialize the decryptor", e);
    }

    byte[] decryptedBytes;

    try {
      crypto.init(Cipher.DECRYPT_MODE, key);
      decryptedBytes = crypto.doFinal(encrypted);
    } catch (Exception e) {
      throw new IOException("Can't decrypt the password", e);
    }

    return new String(decryptedBytes);
  }

  @Override
  public void cleanUpConfiguration(Configuration configuration) {
    // Usage of Configuration#unset would be much better here, sadly
    // this particular API is not available in Hadoop 0.20 and < 1.2.0
    // that we are still supporting. Hence we are overriding the configs
    // with default values.
    configuration.set(PROPERTY_CRYPTO_PASSPHRASE, "REMOVED");
    configuration.set(PROPERTY_CRYPTO_SALT, DEFAULT_SALT);
    configuration.setInt(PROPERTY_CRYPTO_KEY_LEN, DEFAULT_KEY_LEN);
    configuration.setInt(PROPERTY_CRYPTO_ITERATIONS, DEFAULT_ITERATIONS);
  }
}
