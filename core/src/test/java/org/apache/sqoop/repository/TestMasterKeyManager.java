 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.repository;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.model.MMasterKey;
import org.apache.sqoop.security.SecurityConstants;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestMasterKeyManager {
  private MasterKeyManager masterKeyManager;
  private RepositoryManager repositoryManagerMock;
  private Repository jdbcRepoMock;
  private Map<String, String> configurationMap;

  private static final int HMAC_KEY_SIZE_BYTES = 32;
  private static final int CIPHER_KEY_SIZE_BYTES = 16;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    SqoopConfiguration configurationMock = mock(SqoopConfiguration.class);
    configurationMap = new HashMap<>();
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_ENABLED, String
      .valueOf(true));
    configurationMap.put(SecurityConstants
      .REPO_ENCRYPTION_PASSWORD_GENERATOR, "echo youwillnevergetthis");
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM,
      "HmacSHA256");
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM,
      "AES");
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE,
      String.valueOf(CIPHER_KEY_SIZE_BYTES));
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE,
      String.valueOf(CIPHER_KEY_SIZE_BYTES));
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC,
      "AES/CBC/PKCS5Padding");
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM,
      "PBKDF2WithHmacSHA1");
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS,
      "4000");
    doReturn(new MapContext(configurationMap)).when(configurationMock)
      .getContext();
    SqoopConfiguration.setInstance(configurationMock);

    repositoryManagerMock = mock(RepositoryManager.class);
    RepositoryManager.setInstance(repositoryManagerMock);


    jdbcRepoMock = mock(JdbcRepository.class);
    when(jdbcRepoMock.getMasterKey()).thenReturn(null);
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);

    masterKeyManager = MasterKeyManager.getInstance();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    masterKeyManager.destroy();

  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*No Master Key found")
  public void testInitializeWithoutKeyCreationWithoutExistingKey() {
    masterKeyManager.initialize(false);
  }

  @Test
  public void testInitializeWithoutKeyCreationWithExistingKey() {
    masterKeyManager.initialize();

    ArgumentCaptor<MMasterKey> mMasterKeyArgumentCaptor = ArgumentCaptor
      .forClass(MMasterKey.class);
    verify(jdbcRepoMock, times(1)).createMasterKey(mMasterKeyArgumentCaptor
      .capture());

    // Encrypt something with that master key
    String secret = "imasecret";
    String iv = masterKeyManager.generateRandomIv();
    String encrypted = masterKeyManager
      .encryptWithMasterKey(secret, iv);

    masterKeyManager.destroy();

    // Create a new MasterKeyManager instance with existing master key
    // coming from the "db"
    jdbcRepoMock = mock(JdbcRepository.class);
    when(jdbcRepoMock.getMasterKey()).thenReturn(mMasterKeyArgumentCaptor
      .getValue());
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);

    masterKeyManager.initialize();
    verify(jdbcRepoMock, times(1)).getMasterKey();

    // Try to decrypt
    assertEquals(masterKeyManager.decryptWithMasterKey(encrypted, iv, masterKeyManager.generateHmacWithMasterHmacKey(encrypted)), secret);
  }

  @Test
  public void testInitializeWithKeyCreationWithoutExistingKey() {
    masterKeyManager.initialize();

    verify(jdbcRepoMock, times(1)).createMasterKey(any(MMasterKey.class));
  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*HMAC validation failed for Master Key"
  )
  public void testMasterKeyWithInvalidHmac() {
    jdbcRepoMock = mock(JdbcRepository.class);
    when(jdbcRepoMock.getMasterKey()).thenReturn(new MMasterKey(
      Base64.encodeBase64String(generateRandomByteArray(CIPHER_KEY_SIZE_BYTES)),
      Base64.encodeBase64String(generateRandomByteArray(HMAC_KEY_SIZE_BYTES)),
      Base64.encodeBase64String(generateRandomByteArray(CIPHER_KEY_SIZE_BYTES)),
      Base64.encodeBase64String(generateRandomByteArray(CIPHER_KEY_SIZE_BYTES))
    ));
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);

    masterKeyManager.initialize();
  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*No password or password generator set")
  public void testNoPasswordOrGenerator() {
    configurationMap.put(SecurityConstants
      .REPO_ENCRYPTION_PASSWORD_GENERATOR, StringUtils.EMPTY);

    masterKeyManager.initialize();
  }

  @Test
  public void testEncryptAndDecryptWithMasterKey() {
    masterKeyManager.initialize();

    String secret = "imasecret";
    String iv = masterKeyManager.generateRandomIv();
    String encrypted = masterKeyManager
      .encryptWithMasterKey(secret, iv);

    assertEquals(masterKeyManager.decryptWithMasterKey(encrypted, iv, masterKeyManager.generateHmacWithMasterHmacKey(encrypted)), secret);
  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*HMAC validation failed for input")
  public void testEncryptAndDecryptWithMasterKeyWithInvalidHmac() {
    masterKeyManager.initialize();

    String secret = "imasecret";
    String iv = masterKeyManager.generateRandomIv();
    String encrypted = masterKeyManager.encryptWithMasterKey(secret, iv);

    String invalidHmac = Base64.encodeBase64String(generateRandomByteArray(HMAC_KEY_SIZE_BYTES));
    masterKeyManager.decryptWithMasterKey(encrypted, iv, invalidHmac);
  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*Invalid configuration.*" +
      SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM)
  public void testMissingConfiguration() {
    configurationMap.put(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM, "");
    masterKeyManager.initialize();
  }

  private static byte[] generateRandomByteArray(int size) {
    byte[] randomBytes = new byte[size];
    new Random().nextBytes(randomBytes);
    return randomBytes;
  }
}
