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
package org.apache.sqoop.repository;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.model.MMasterKey;
import org.apache.sqoop.security.SecurityConstants;
import org.apache.sqoop.security.SecurityError;
import org.apache.sqoop.utils.PasswordUtils;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

public class MasterKeyManager {

  private String hmacAlgorithm;
  private int hmacKeySizeBytes;
  private String cipherAlgorithm;
  private int cipherKeySize;
  private String cipherSpec;
  private String pbkdf2Algorithm;
  private int pbkdf2Rounds;
  private int ivLength;

  private RepositoryTransaction repositoryTransaction;
  private MMasterKey mMasterKey;

  private SecretKey masterEncryptionKey;
  private SecretKey masterHmacKey;

  private Random random;

  private static MasterKeyManager instance;

  static {
    instance = new MasterKeyManager();
  }

  public MasterKeyManager() {
  }

  public static MasterKeyManager getInstance() {
    return instance;
  }

  public static void setInstance(MasterKeyManager newInstance) {
    instance = newInstance;
  }

  public void initialize() throws SqoopException {
    initialize(true);
  }

  public void initialize(boolean createMasterKey) throws SqoopException {
    initialize(createMasterKey, false, null);
  }

  public void initialize(boolean createMasterKey, boolean createKeyEvenIfKeyExists, RepositoryTransaction repositoryTransactionArg) throws SqoopException {
    MapContext configurationContext = SqoopConfiguration.getInstance().getContext();
    if (configurationContext.getBoolean(SecurityConstants.REPO_ENCRYPTION_ENABLED, false)) {

      // Grab configuration from the sqoop properties file. All of this configuration is required
      // and an exception will be thrown if any of it is missing
      String hmacAlgorithm = populateStringConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM);
      String cipherAlgorithm = populateStringConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM);
      String cipherSpec = populateStringConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC);
      int cipherKeySize = populateIntConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE);
      int ivLength = populateIntConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE);
      String pbkdf2Algorithm = populateStringConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM);
      int pbkdf2Rounds = populateIntConfiguration(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS);

      String password = PasswordUtils.readPassword(configurationContext,
        SecurityConstants.REPO_ENCRYPTION_PASSWORD,
        SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR);

      initialize(createMasterKey, hmacAlgorithm, cipherAlgorithm, cipherSpec, cipherKeySize,
        ivLength, pbkdf2Algorithm, pbkdf2Rounds, password, createKeyEvenIfKeyExists, repositoryTransactionArg);
    }
  }


  public synchronized void initialize(boolean createMasterKey, String hmacAlgorithmArg,
                                      String cipherAlgorithmArg, String cipherSpecArg,
                                      int cipherKeySizeArg, int ivLengthArg,
                                      String pbkdf2AlgorithmArg, int pbkdf2RoundsArg,
                                      String password, boolean createKeyEvenIfKeyExists, RepositoryTransaction repositoryTransactionArg) throws SqoopException {
    hmacAlgorithm = hmacAlgorithmArg;
    cipherAlgorithm = cipherAlgorithmArg;
    cipherSpec = cipherSpecArg;
    cipherKeySize = cipherKeySizeArg;
    ivLength = ivLengthArg;
    pbkdf2Algorithm = pbkdf2AlgorithmArg;
    pbkdf2Rounds = pbkdf2RoundsArg;

    repositoryTransaction = repositoryTransactionArg;

    // This is used for the generation of random initialization vectors and salts
    random = new SecureRandom();
    // The size of the hmac key can be derived from the provided HMAC algorithm
    try {
      hmacKeySizeBytes = Mac.getInstance(hmacAlgorithm).getMacLength();
    } catch (NoSuchAlgorithmException e) {
      throw new SqoopException(SecurityError.ENCRYPTION_0011, e);
    }

    Repository repository = RepositoryManager.getInstance().getRepository();
    if (StringUtils.isEmpty(password)) {
      throw new SqoopException(SecurityError.ENCRYPTION_0008);
    }

    MMasterKey existingEncryptedMasterKey = repository.getMasterKey(repositoryTransaction);
    String salt;

    if (existingEncryptedMasterKey == null || createKeyEvenIfKeyExists) {
      // Since the master key does not exist, we can generate a random salt that we will use
      // for encryption of the Master Key
      // We will use a salt that is the same size as the encryption key
      salt = Base64.encodeBase64String(generateRandomByteArray(hmacKeySizeBytes));
    } else {
      // Since the master key already exists, we will read the salt from the repository
      salt = existingEncryptedMasterKey.getSalt();
    }

    // Derive two keys (that we will be used to encrypt and verify the master key)
    // from the configuration provided password and the salt we just read/created.
    byte[] keyBytes = getKeysFromPassword(password, salt);
    SecretKey passwordEncryptionKey = new SecretKeySpec(keyBytes, 0,
      cipherKeySize, cipherAlgorithm);
    SecretKey passwordHmacKey = new SecretKeySpec(keyBytes,
      cipherKeySize, hmacKeySizeBytes, hmacAlgorithm);

    byte[] masterEncryptionKeyBytes;
    byte[] masterHmacKeyBytes;
    if (existingEncryptedMasterKey == null || createKeyEvenIfKeyExists) {
      if (createMasterKey) {
        // A master key does not exist so we must create one. We will simply
        // use two random byte arrays for the encryption and hmac components.
        // The sizes of these keys is determined by the values provided to
        // configuration.
        masterEncryptionKeyBytes = generateRandomByteArray(cipherKeySize);
        masterHmacKeyBytes = generateRandomByteArray(hmacKeySizeBytes);

        // The initialization vector for the encryption of the master key is
        // randomly generated.
        String iv = Base64.encodeBase64String(generateRandomByteArray(ivLength));

        // We append our two keys together and encrypt the resulting byte array.
        // This is the secret that all of the encryption in the repository depends upon
        byte[] secret = ArrayUtils.addAll(masterEncryptionKeyBytes, masterHmacKeyBytes);
        String encryptedSecret = encryptToString(passwordEncryptionKey, secret, iv);

        // We store our new master key in the repository in its encrypted form
        // along with an HMAC to verify the key when we read it, the salt needed to
        // generate keys to decrypt it, and the initialization vector used
        mMasterKey = new MMasterKey(encryptedSecret, generateHmac(passwordHmacKey, encryptedSecret), salt, iv);
        repository.createMasterKey(mMasterKey, repositoryTransaction);
      } else {
        // If a master key does not exist and we are trying to initialize the
        // manager without allowing it to create a master key, we should fail
        throw new SqoopException(SecurityError.ENCRYPTION_0002);
      }
    } else {
      // A master key exists so we need to read it from the repository and
      // decrypt it.
      mMasterKey = existingEncryptedMasterKey;
      String iv = existingEncryptedMasterKey.getIv();
      String encryptedSecret = existingEncryptedMasterKey.getEncryptedSecret();

      // Before we go about decrypting the master key we should verify the hmac
      // to ensure that it has not been tampered with
      String hmac = existingEncryptedMasterKey.getHmac();
      if (!validHmac(passwordHmacKey, encryptedSecret, hmac)) {
        throw new SqoopException(SecurityError.ENCRYPTION_0001);
      }

      // The master key has not been tampered with, lets decrypt it using the key
      // derived from the password and the initialization vector from the repository
      byte[] decryptedKey = decryptToBytes(passwordEncryptionKey, encryptedSecret, iv);

      // Since the master key is stored as the concatenation of an encryption
      // key and an hmac key, we need to split it according to the sizes derived
      // from the configuration
      masterEncryptionKeyBytes = new byte[cipherKeySize];
      masterHmacKeyBytes = new byte[hmacKeySizeBytes];
      System.arraycopy(decryptedKey, 0, masterEncryptionKeyBytes, 0,
        cipherKeySize);
      System.arraycopy(decryptedKey, cipherKeySize,
        masterHmacKeyBytes, 0, hmacKeySizeBytes);
    }

    // Place the master encryption and master hmac key in SecretKey objects
    // so we can use them to encrypt and decrypt data
    masterEncryptionKey = new SecretKeySpec(masterEncryptionKeyBytes, 0, cipherKeySize, cipherAlgorithm);
    masterHmacKey = new SecretKeySpec(masterHmacKeyBytes, 0, hmacKeySizeBytes, hmacAlgorithm);
  }

  public synchronized void destroy() {
    hmacAlgorithm = null;
    hmacKeySizeBytes = 0;
    cipherAlgorithm = null;
    cipherKeySize = 0;
    cipherSpec = null;
    pbkdf2Algorithm = null;
    pbkdf2Rounds = 0;
    ivLength = 0;

    repositoryTransaction = null;
    mMasterKey = null;

    masterEncryptionKey = null;
    masterHmacKey = null;

    random = null;
  }

  public void deleteMasterKeyFromRepository() {
    RepositoryManager.getInstance().getRepository().deleteMasterKey(mMasterKey.getPersistenceId(), repositoryTransaction);
  }

  /**
   * Returns a Base64 representation of the encrypted cleartext, using the Master Key
   *
   * @param clearText Text to encrypt
   * @param iv Initialization vector for the cipher
   * @return Base64 representation of the encrypted cleartext
   * @throws SqoopException
   */
  public String encryptWithMasterKey(String clearText, String iv) throws SqoopException {
    return encryptToString(masterEncryptionKey, clearText, iv);
  }

  /**
   * Validates the HMAC against the cipher text and then returns a UTF-8 string
   * of the data decrypted using the Master Key
   *
   * Throws an exception of the HMAC is incorrect
   *
   * @param cipherText Base64 representation of the encrypted text
   * @param iv Initialization vector for the cipher
   * @param hmac HMAC for tamper resistance
   * @return UTF-8 string from the decrypted data
   * @throws SqoopException
   */
  public String decryptWithMasterKey(String cipherText, String iv, String hmac) throws SqoopException {
    if (!validWithMasterHmacKey(cipherText, hmac)) {
      throw new SqoopException(SecurityError.ENCRYPTION_0010);
    }
    return decryptWithMasterKey(cipherText, iv);
  }

  /**
   * Generates the hmac for the provided cipher text using the Master Key
   *
   * @param cipherText Base64 representation of the encrypted text
   * @return Base64 representation of the HMAC
   * @throws SqoopException
   */
  public String generateHmacWithMasterHmacKey(String cipherText) throws SqoopException {
    return generateHmac(masterHmacKey, cipherText);
  }

  /**
   * Generates a random initialization vector of the expected size
   * @return Base64 representation of the initialization vector
   */
  public String generateRandomIv() {
    return Base64.encodeBase64String(generateRandomByteArray(ivLength));
  }

  /**
   * Determines if the provided HMAC matches the HMAC generated for the cipher text
   * using the Master Key
   *
   * @param cipherText Base64 representation of the encrypted data
   * @param expectedHmac Provided HMAC to compare against
   * @return True if expectedHmac matches what is generated by the Master Key, false otherwise
   * @throws SqoopException
   */
  private boolean validWithMasterHmacKey(String cipherText, String expectedHmac) throws SqoopException {
    return validHmac(masterHmacKey, cipherText, expectedHmac);
  }

  /**
   * Decrypts the provided ciphertext with the IV provided and the Master Key
   *
   * @param cipherText Base64 representation of the encrypted data
   * @param iv Initialization vector for use by the cipher
   * @return UTF-8 representation of the decrypted data
   * @throws SqoopException
   */
  private String decryptWithMasterKey(String cipherText, String iv) throws SqoopException {
    return decryptToString(masterEncryptionKey, cipherText, iv);
  }

  /**
   * Encrypts the provided cleartext with the provided IV and SecretKey
   *
   * @param secretKey Key to use for the encryption
   * @param clearText String that is to be encrypted
   * @param iv Initialization vector for use by the cipher,
   * @return byte array representing the encrypted data
   * @throws SqoopException
   */
  private byte[] encryptToBytes(SecretKey secretKey, String clearText, String iv) throws SqoopException {
    return encryptToBytes(secretKey, clearText.getBytes(Charset.forName("UTF-8")), iv);
  }

  /**
   * Encrypts the provided cleartext with the provided IV and SecretKey
   *
   * @param secretKey Key to use for the encryption
   * @param clearText Byte array that is to be encrypted
   * @param iv Initialization vector for use by the cipher,
   * @return byte array representing the encrypted data
   * @throws SqoopException
   */
  private byte[] encryptToBytes(SecretKey secretKey, byte[] clearText, String iv) throws SqoopException {
    try {
      Cipher cipher = Cipher.getInstance(cipherSpec);
      cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(Base64.decodeBase64(iv)));
      return cipher.doFinal(clearText);
    } catch (GeneralSecurityException exception) {
      throw new SqoopException(SecurityError.ENCRYPTION_0004, exception);
    }
  }

  /**
   * Encrypts the provided cleartext with the provided IV and SecretKey
   *
   * @param secretKey Key to use for the encryption
   * @param clearText Byte array that is to be encrypted
   * @param iv Initialization vector for use by the cipher,
   * @return Base64 representation of the encrypted data
   * @throws SqoopException
   */
  private String encryptToString(SecretKey secretKey, byte[] clearText, String iv) throws SqoopException {
    return Base64.encodeBase64String(encryptToBytes(secretKey, clearText, iv));
  }

  /**
   * Encrypts the provided cleartext with the provided IV and SecretKey
   *
   * @param secretKey Key to use for the encryption
   * @param clearText String that is to be encrypted
   * @param iv Initialization vector for use by the cipher,
   * @return Base64 representation of the encrypted data
   * @throws SqoopException
   */
  private String encryptToString(SecretKey secretKey, String clearText, String iv) throws SqoopException {
    return Base64.encodeBase64String(encryptToBytes(secretKey, clearText, iv));
  }

  /**
   * Validates an HMAC against some cipherText using the provided hmacKey
   *
   * @param hmacKey SecretKey which defines the HMAC key and the HMAC algorithm
   * @param cipherText Encrypted text from which the HMAC is generated
   * @param expectedHmac The expected value for the HMAC that we will be comparing against
   * @return True if the generated HMAC matches the expectedHmac, false otherwise
   * @throws SqoopException
   */
  private boolean validHmac(SecretKey hmacKey, String cipherText, String expectedHmac) throws SqoopException {
    try {
      Mac hmac = Mac.getInstance(hmacAlgorithm);
      hmac.init(hmacKey);
      byte[] calculatedHmac = hmac.doFinal(Base64.decodeBase64(cipherText));
      return Arrays.equals(calculatedHmac, Base64.decodeBase64(expectedHmac));
    } catch (GeneralSecurityException exception) {
      throw new SqoopException(SecurityError.ENCRYPTION_0005, exception);
    }
  }

  /**
   * Generates an HMAC based on a SecretKey and some cipherText
   *
   * @param hmacKey SecretKey which defines the HMAC key and the HMAC algorithm
   * @param cipherText Encrypted text from which the HMAC is generated
   * @return Base64 representation of the HMAC value
   * @throws SqoopException
   */
  private String generateHmac(SecretKey hmacKey, String cipherText) throws SqoopException {
    try {
      Mac hmac = Mac.getInstance(hmacAlgorithm);
      hmac.init(hmacKey);
      return Base64.encodeBase64String(hmac.doFinal(Base64.decodeBase64(cipherText)));
    } catch (GeneralSecurityException exception) {
      throw new SqoopException(SecurityError.ENCRYPTION_0005, exception);
    }
  }

  /**
   * Decrypts the provided cipherText using the provided encryptionKey and iv
   *
   * @param encryptionKey SecretKey which defines the encryption key and algorithm
   * @param cipherText Base64 representation of the data we want to decrypt
   * @param iv Base64 representation of the initialization vector used when the data was encrypted
   * @return Byte array representing the decrypted data
   * @throws SqoopException
   */
  private byte[] decryptToBytes(SecretKey encryptionKey, String cipherText, String iv) throws SqoopException {
    try {
      Cipher cipher = Cipher.getInstance(cipherSpec);
      cipher.init(Cipher.DECRYPT_MODE, encryptionKey, new IvParameterSpec(Base64.decodeBase64(iv)));
      return cipher.doFinal(Base64.decodeBase64(cipherText));
    } catch (GeneralSecurityException exception) {
      throw new SqoopException(SecurityError.ENCRYPTION_0006, exception);
    }
  }

  /**
   * Decrypts the provided cipherText using the provided encryptionKey and iv
   *
   * @param encryptionKey SecretKey which defines the encryption key and algorithm
   * @param cipherText Base64 representation of the data we want to decrypt
   * @param iv Base64 representation of the initialization vector used when the data was encrypted
   * @return String representing the decrypted data using the UTF-8 character set
   * @throws SqoopException
   */
  private String decryptToString(SecretKey encryptionKey, String cipherText, String iv) throws SqoopException {
    return new String(decryptToBytes(encryptionKey, cipherText, iv), Charset.forName("UTF-8"));
  }

  /**
   * Reads the specified String configuration value from the provided configurationContext,
   * throws an exception if the value cannot be found
   *
   * @param configurationContext MapContext containing the sqoop configuration
   * @param configuration Configuration value that we would like from the configurationContext
   * @return String value from the configuration
   * @throws SqoopException
   */
  private String populateStringConfiguration(MapContext configurationContext, String configuration) throws SqoopException {
    String value = configurationContext.getString(configuration);
    if (StringUtils.isEmpty(value)){
      throw new SqoopException(SecurityError.ENCRYPTION_0009, configuration);
    }
    return value;
  }

  /**
   * Reads the specified integer configuration value from the provided configurationContext,
   * throws an exception if the value cannot be found
   *
   * @param configurationContext MapContext containing the sqoop configuration
   * @param configuration Configuration value that we would like from the configurationContext
   * @return int value from the configuration
   * @throws SqoopException
   */
  private int populateIntConfiguration(MapContext configurationContext, String configuration) throws SqoopException {
    int value = configurationContext.getInt(configuration, 0);
    if (value < 1){
      throw new SqoopException(SecurityError.ENCRYPTION_0009, configuration);
    }
    return value;
  }

  /**
   * Generates a random byte array of the specified length
   *
   * @param size number of random bytes to return
   * @return byte array containing random bytes of the specified size
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("IS2_INCONSISTENT_SYNC")
  private byte[] generateRandomByteArray(int size) {
    byte[] randomBytes = new byte[size];
    random.nextBytes(randomBytes);
    return randomBytes;
  }

  /**
   * Using the PBKDF2 algorithm, we will generate a Master Key.
   *
   * @param password The password that will be used for the generation of encryption keys
   * @param salt Salt to be used for the generation of encryption keys
   * @return byte[] containing the key generated by PDKDF2
   * @throws SqoopException
   */
  private byte[] getKeysFromPassword(String password, String salt) throws SqoopException {
    try {
      PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), Base64.decodeBase64(salt),
        pbkdf2Rounds, (cipherKeySize + hmacKeySizeBytes) * 8);
      SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(pbkdf2Algorithm);
      return secretKeyFactory.generateSecret(spec).getEncoded();
    } catch (GeneralSecurityException exception) {
      throw new SqoopException(SecurityError.ENCRYPTION_0003, exception);
    }
  }
}
