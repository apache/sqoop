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
package org.apache.sqoop.integration.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.core.PropertiesConfigurationProvider;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.repository.MasterKeyManager;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.repository.common.CommonRepoUtils;
import org.apache.sqoop.repository.common
  .CommonRepositoryInsertUpdateDeleteSelectQuery;
import org.apache.sqoop.security.SecurityConstants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.tools.tool.RepositoryEncryptionTool;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_ENCRYPTED;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_HMAC;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_INPUT;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_IV;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQ_LNKI_VALUE;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_LINK_INPUT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups = "no-real-cluster")
@Infrastructure(dependencies = {DatabaseInfrastructureProvider.class, HadoopInfrastructureProvider.class})
public class RepositoryEncryptionToolTest extends SqoopTestCase {

  private SqoopMiniCluster sqoopMiniCluster;
  private String temporaryPath;

  public static final String JDBC_URL = "jdbc:derby:memory:myDB";
  public static final String INPUT_VALUE_QUERY =
    "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + ", "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_VALUE) + ", "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_ENCRYPTED) + ", "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_IV) + ", "
    + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_HMAC)
    + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME)
    + " WHERE " +  CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + " = ?";

  private String passwordGenerator;
  private String hmacAlgorithm;
  private String cipherAlgorithm;
  private int cipherKeySize;
  private String cipherSpec;
  private String pbkdf2Algorithm;
  private int pbkdf2Rounds;
  private int ivLength;

  private MStringInput sensitiveInput;
  private MMapInput sensitiveKeyPatternInput;

  public static class SqoopMiniCluster extends JettySqoopMiniCluster {

    private boolean repositoryEncryptionEnabled;

    private String passwordGenerator;
    private String hmacAlgorithm;
    private String cipherAlgorithm;
    private int cipherKeySize;
    private String cipherSpec;
    private String pbkdf2Algorithm;
    private int pbkdf2Rounds;
    private int ivLength;

    public SqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
      super(temporaryPath, configuration);
      this.repositoryEncryptionEnabled = false;
    }

    public SqoopMiniCluster(String temporaryPath, Configuration configuration, String passwordGenerator, String hmacAlgorithm, String cipherAlgorithm, int cipherKeySize, String cipherSpec, String pbkdf2Algorithm, int pbkdf2Rounds, int ivLength) throws Exception {
      super(temporaryPath, configuration);
      this.repositoryEncryptionEnabled = true;
      this.passwordGenerator = passwordGenerator;
      this.hmacAlgorithm = hmacAlgorithm;
      this.cipherAlgorithm = cipherAlgorithm;
      this.cipherKeySize = cipherKeySize;
      this.cipherSpec = cipherSpec;
      this.pbkdf2Algorithm = pbkdf2Algorithm;
      this.pbkdf2Rounds = pbkdf2Rounds;
      this.ivLength = ivLength;
    }

    @Override
    protected Map<String, String> getSecurityConfiguration() {
      Map<String, String> properties = super.getSecurityConfiguration();

      // Remove all default repository encryption values
      properties.remove(SecurityConstants.REPO_ENCRYPTION_ENABLED);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM);
      properties.remove(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS);

      properties.put(SecurityConstants.REPO_ENCRYPTION_ENABLED, String.valueOf(repositoryEncryptionEnabled));
      if (repositoryEncryptionEnabled) {
        properties.put(SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR, passwordGenerator);
        properties.put(SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM, hmacAlgorithm);
        properties.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM, cipherAlgorithm);
        properties.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE, String.valueOf(cipherKeySize));
        properties.put(SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE, String.valueOf(ivLength));
        properties.put(SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC, cipherSpec);
        properties.put(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM, pbkdf2Algorithm);
        properties.put(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS, String.valueOf(pbkdf2Rounds));
      }

      return properties;
    }

    public boolean isRepositoryEncryptionEnabled() {
      return repositoryEncryptionEnabled;
    }
  }

  @BeforeMethod
  public void before() throws Exception {
    dropRepository();

    passwordGenerator = "echo test";
    hmacAlgorithm = "HmacSHA256";
    cipherAlgorithm = "AES";
    cipherKeySize = 16;
    cipherSpec = "AES/CBC/PKCS5Padding";
    pbkdf2Algorithm = "PBKDF2WithHmacSHA1";
    pbkdf2Rounds = 4000;
    ivLength = 16;

    temporaryPath = HdfsUtils.joinPathFragments(super.getTemporaryPath(), getTestName());
  }

  @Test
  public void testNotEncryptedToEncrypted() throws Exception {
    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf());
    sqoopMiniCluster.start();

    verifyMasterKeyDoesNotExist();

    createInputsAndJob();

    // Stop sqoop instance
    sqoopMiniCluster.stop();

    // Run tool
    RepositoryEncryptionTool repositoryEncryptionTool = new RepositoryEncryptionTool();
    repositoryEncryptionTool.runToolWithConfiguration(new String[] {
      "-T" + SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR + "=" + passwordGenerator,
      "-T" + SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM + "=" + hmacAlgorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM + "=" + cipherAlgorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE + "=" + cipherKeySize,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC + "=" + cipherSpec,
      "-T" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM + "=" + pbkdf2Algorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS + "=" + pbkdf2Rounds,
      "-T" + SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE + "=" + ivLength,
    });

    cleanUpAfterTool();

    // Read the encrypted data by using the MasterKeyManager the server initializes
    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    verifyEncryptedInput(sensitiveInput);
    verifyEncryptedInput(sensitiveKeyPatternInput);
  }

  @Test
  public void testEncryptedToNotEncrypted() throws Exception {
    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    createInputsAndJob();

    verifyEncryptedInput(sensitiveInput);
    verifyEncryptedInput(sensitiveKeyPatternInput);

    sqoopMiniCluster.stop();

    // Run tool
    RepositoryEncryptionTool repositoryEncryptionTool = new RepositoryEncryptionTool();
    repositoryEncryptionTool.runToolWithConfiguration(new String[] {
      "-F" + SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR + "=" + passwordGenerator,
      "-F" + SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM + "=" + hmacAlgorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM + "=" + cipherAlgorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE + "=" + cipherKeySize,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC + "=" + cipherSpec,
      "-F" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM + "=" + pbkdf2Algorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS + "=" + pbkdf2Rounds,
      "-F" + SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE + "=" + ivLength,
    });

    cleanUpAfterTool();

    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf());
    sqoopMiniCluster.start();

    verifyPlaintextInput(sensitiveInput);
    verifyPlaintextInput(sensitiveKeyPatternInput);

    verifyMasterKeyDoesNotExist();
  }

  @Test
  public void testEncryptedToEncrypted() throws Exception {
    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    createInputsAndJob();

    String fromSensitiveCiphertext = verifyEncryptedInput(sensitiveInput);
    String fromSensitiveKeyPatternCiphertext = verifyEncryptedInput(sensitiveKeyPatternInput);

    sqoopMiniCluster.stop();

    RepositoryEncryptionTool repositoryEncryptionTool = new RepositoryEncryptionTool();
    repositoryEncryptionTool.runToolWithConfiguration(new String[] {
      "-F" + SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR + "=" + passwordGenerator,
      "-F" + SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM + "=" + hmacAlgorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM + "=" + cipherAlgorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE + "=" + cipherKeySize,
      "-F" + SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC + "=" + cipherSpec,
      "-F" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM + "=" + pbkdf2Algorithm,
      "-F" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS + "=" + pbkdf2Rounds,
      "-F" + SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE + "=" + ivLength,

      "-T" + SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR + "=" + passwordGenerator,
      "-T" + SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM + "=" + hmacAlgorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM + "=" + cipherAlgorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE + "=" + cipherKeySize,
      "-T" + SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC + "=" + cipherSpec,
      "-T" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM + "=" + pbkdf2Algorithm,
      "-T" + SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS + "=" + pbkdf2Rounds,
      "-T" + SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE + "=" + ivLength,
    });

    cleanUpAfterTool();

    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    String toSesitiveCipherText = verifyEncryptedInput(sensitiveInput);
    String toSensitiveKeyPatternCiphertext = verifyEncryptedInput(sensitiveKeyPatternInput);

    Assert.assertNotEquals(fromSensitiveCiphertext, toSesitiveCipherText);
    Assert.assertNotEquals(fromSensitiveKeyPatternCiphertext, toSensitiveKeyPatternCiphertext);
  }

  @Test
  public void testEncryptedToEncryptedUsingConfiguration() throws Exception {
    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    createInputsAndJob();

    String fromSensitiveCiphertext = verifyEncryptedInput(sensitiveInput);
    String fromSensitiveKeyPatternCiphertext = verifyEncryptedInput(sensitiveKeyPatternInput);

    // Read the configuration context that we will need for the tool
    MapContext configurationMapContext = SqoopConfiguration.getInstance().getContext();

    sqoopMiniCluster.stop();

    // Set the configuration
    SqoopConfiguration oldSqoopConfiguration = SqoopConfiguration.getInstance();
    SqoopConfiguration configurationMock = mock(SqoopConfiguration.class);
    when(configurationMock.getContext()).thenReturn(configurationMapContext);
    when(configurationMock.getProvider()).thenReturn(new PropertiesConfigurationProvider());
    SqoopConfiguration.setInstance(configurationMock);
    RepositoryEncryptionTool repositoryEncryptionTool = new RepositoryEncryptionTool();
    repositoryEncryptionTool.runToolWithConfiguration(new String[] {
      "-FuseConf",
      "-TuseConf",
    });

    cleanUpAfterTool();

    sqoopMiniCluster = new SqoopMiniCluster(temporaryPath, getHadoopConf(), passwordGenerator,
      hmacAlgorithm, cipherAlgorithm, cipherKeySize, cipherSpec, pbkdf2Algorithm, pbkdf2Rounds, ivLength);
    sqoopMiniCluster.start();

    String toSesitiveCipherText = verifyEncryptedInput(sensitiveInput);
    String toSensitiveKeyPatternCiphertext = verifyEncryptedInput(sensitiveKeyPatternInput);

    Assert.assertNotEquals(fromSensitiveCiphertext, toSesitiveCipherText);
    Assert.assertNotEquals(fromSensitiveKeyPatternCiphertext, toSensitiveKeyPatternCiphertext);

    SqoopConfiguration.setInstance(oldSqoopConfiguration);
  }

  private void createInputsAndJob() throws Exception {
    SqoopClient client = new SqoopClient(sqoopMiniCluster.getServerUrl());
    MLink jdbcLink = client.createLink("generic-jdbc-connector");
    jdbcLink.setName("jdbcLink");
    fillRdbmsLinkConfig(jdbcLink);
    client.saveLink(jdbcLink);

    MLink hdfsLink = client.createLink("hdfs-connector");
    hdfsLink.setName("hdfsLink");
    hdfsLink.getConnectorLinkConfig().getStringInput("linkConfig.confDir").setValue((sqoopMiniCluster.getConfigurationPath()));
    Map<String, String> hdfsConfigOverrides = new HashMap<>();

    // This will be considered sensitive
    hdfsConfigOverrides.put("password", "secret");
    hdfsLink.getConnectorLinkConfig().getMapInput("linkConfig.configOverrides").setValue(hdfsConfigOverrides);

    client.saveLink(hdfsLink);

    sensitiveInput = jdbcLink.getConnectorLinkConfig().getStringInput("linkConfig.password");

    sensitiveKeyPatternInput = hdfsLink.getConnectorLinkConfig().getMapInput("linkConfig.configOverrides");

    if (sqoopMiniCluster.isRepositoryEncryptionEnabled()) {
      verifyEncryptedInput(sensitiveInput);
      verifyEncryptedInput(sensitiveKeyPatternInput);
    } else {
      verifyPlaintextInput(sensitiveInput);
      verifyPlaintextInput(sensitiveKeyPatternInput);
    }

    MJob job = client.createJob(jdbcLink.getName(), hdfsLink.getName());
    job.setName("job");
    job.getDriverConfig().getIntegerInput("throttlingConfig.numExtractors").setValue(1);
    fillRdbmsFromConfig(job, "id");
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    client.saveJob(job);
  }

  private void cleanUpAfterTool() {
    ConnectorManager.getInstance().destroy();
    RepositoryManager.getInstance().destroy();
    MasterKeyManager.getInstance().destroy();
    SqoopConfiguration.getInstance().destroy();
  }

  private void verifyMasterKeyDoesNotExist() throws Exception {
    try (PreparedStatement inputSelection = DriverManager.getConnection(JDBC_URL).prepareStatement((new CommonRepositoryInsertUpdateDeleteSelectQuery()).getStmtSelectSqMasterKey())) {
      try (ResultSet resultSet = inputSelection.executeQuery()) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  private String verifyEncryptedInput(MInput<?> input) throws Exception {
    StringBuffer cipherText = new StringBuffer();
    StringBuffer iv = new StringBuffer();
    StringBuffer hmac = new StringBuffer();
    readEncryptedInput(input.getPersistenceId(), cipherText, iv, hmac);

    String sensitiveDecrypted = MasterKeyManager.getInstance().decryptWithMasterKey(cipherText.toString(), iv.toString(), hmac.toString());
    Assert.assertEquals(input.getUrlSafeValueString(), sensitiveDecrypted);

    return cipherText.toString();
  }

  private void verifyPlaintextInput(MInput<?> input) throws Exception {
    try (PreparedStatement inputSelection = DriverManager.getConnection(JDBC_URL).prepareStatement(INPUT_VALUE_QUERY)) {
      inputSelection.setLong(1, input.getPersistenceId());
      try (ResultSet resultSet = inputSelection.executeQuery()) {
        while (resultSet.next()) {
          Assert.assertEquals(input.getUrlSafeValueString(), resultSet.getString(2));
          Assert.assertFalse(resultSet.getBoolean(3));
          Assert.assertNull(resultSet.getString(4));
          Assert.assertNull(resultSet.getString(5));
        }
      }
    }
  }

  private void readEncryptedInput(long inputId, StringBuffer cipherText, StringBuffer iv, StringBuffer hmac) throws Exception {
    try (PreparedStatement inputSelection = DriverManager.getConnection(JDBC_URL).prepareStatement(INPUT_VALUE_QUERY)) {
      inputSelection.setLong(1, inputId);
      try (ResultSet resultSet = inputSelection.executeQuery()) {
        while (resultSet.next()) {
          Assert.assertTrue(resultSet.getBoolean(3));
          cipherText.append(resultSet.getString(2));
          iv.append(resultSet.getString(4));
          hmac.append(resultSet.getString(5));
        }
      }
    }
  }

  @AfterMethod
  public void stopCluster() throws Exception {
    sqoopMiniCluster.stop();
    dropRepository();
  }

  private void dropRepository() {
    try {
      DriverManager.getConnection(JDBC_URL + ";drop=true");
    } catch (Exception exception) {
      // Dropping the database always throws an exception
    }
  }
}
