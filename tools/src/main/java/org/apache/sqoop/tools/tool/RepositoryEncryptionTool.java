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
package org.apache.sqoop.tools.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.sqoop.cli.SqoopGnuParser;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.repository.MasterKeyManager;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.repository.RepositoryTransaction;
import org.apache.sqoop.security.SecurityConstants;
import org.apache.sqoop.security.SecurityError;
import org.apache.sqoop.tools.ConfiguredTool;
import org.apache.sqoop.utils.PasswordUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RepositoryEncryptionTool extends ConfiguredTool {
  public static final Logger LOG = Logger.getLogger(RepositoryDumpTool.class);

  private static String FROM_OPTION = "F";
  private static String TO_OPTION = "T";

  private static String USE_CONFIGURATION = "useConf";

  @Override
  public boolean runToolWithConfiguration(String[] arguments) {
    Options options = new Options();
    options.addOption(OptionBuilder.hasArgs().withValueSeparator().create(FROM_OPTION));
    options.addOption(OptionBuilder.hasArgs().withValueSeparator().create(TO_OPTION));

    CommandLineParser parser = new SqoopGnuParser();
    SqoopConfiguration.getInstance().initialize();
    RepositoryManager.getInstance().initialize();
    Repository repository = RepositoryManager.getInstance().getRepository();

    CommandLine line;
    try {
      line = parser.parse(options, arguments);
    } catch (ParseException e) {
      LOG.error("Error parsing command line arguments:", e);
      System.out.println("Error parsing command line arguments. Please check Server logs for details.");
      return false;
    }

    Properties fromProperties = line.getOptionProperties(FROM_OPTION);
    Properties toProperties = line.getOptionProperties(TO_OPTION);

    RepositoryTransaction repositoryTransaction = null;
    boolean successful = true;
    try {
      repositoryTransaction = repository.getTransaction();
      repositoryTransaction.begin();

      MasterKeyManager fromMasterKeyManager = null;
      MasterKeyManager toMasterKeyManager = null;

      if (!fromProperties.isEmpty()) {
        fromMasterKeyManager = initializeMasterKeyManagerFromProperties(fromProperties, false, repositoryTransaction);
      } else {
        // Check to make sure there is no master key to prevent corruption
        if (repository.getMasterKey(repositoryTransaction) != null) {
          System.out.println("Repository is encrypted, need configuration to decrypt");
          throw new SqoopException(SecurityError.ENCRYPTION_0013);
        }
      }

      if (!toProperties.isEmpty()) {
        toMasterKeyManager = initializeMasterKeyManagerFromProperties(toProperties, true, repositoryTransaction);
      }

      repository.changeMasterKeyManager(fromMasterKeyManager, toMasterKeyManager, repositoryTransaction);
      if (fromMasterKeyManager != null) {
        fromMasterKeyManager.deleteMasterKeyFromRepository();
        fromMasterKeyManager.destroy();
      }

      repositoryTransaction.commit();
      System.out.println("Changes committed");
    } catch (Exception ex) {
      if (repositoryTransaction != null) {
        repositoryTransaction.rollback();
      }
      System.out.println("Error running tool. Please check Server logs for details.");
      LOG.error(new SqoopException(SecurityError.ENCRYPTION_0012, ex));
      successful = false;
    } finally {
      if (repositoryTransaction != null) {
        repositoryTransaction.close();
      }
    }
    return successful;
  }

  private MasterKeyManager initializeMasterKeyManagerFromProperties(Properties properties, boolean readFromRepository, RepositoryTransaction transaction) {
    MasterKeyManager masterKeyManager = new MasterKeyManager();
    if (properties.getProperty(USE_CONFIGURATION) != null) {
      masterKeyManager.initialize(true, readFromRepository, transaction);
    } else {
      String hmacAlgorithm = properties.getProperty(SecurityConstants.REPO_ENCRYPTION_HMAC_ALGORITHM);
      String cipherAlgorithm = properties.getProperty(SecurityConstants.REPO_ENCRYPTION_CIPHER_ALGORITHM);
      String cipherSpec = properties.getProperty(SecurityConstants.REPO_ENCRYPTION_CIPHER_SPEC);
      int cipherKeySize = Integer.parseInt(properties.getProperty(SecurityConstants.REPO_ENCRYPTION_CIPHER_KEY_SIZE));
      int ivLength = Integer.parseInt(properties.getProperty(SecurityConstants.REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE));
      String pbkdf2Algorithm = properties.getProperty(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ALGORITHM);
      int pbkdf2Rounds = Integer.parseInt(properties.getProperty(SecurityConstants.REPO_ENCRYPTION_PBKDF2_ROUNDS));

      // We need to create a MapContext to make reading the password simpler here
      Map<String, String> passwordProperties = new HashMap<>();
      passwordProperties.put(SecurityConstants.REPO_ENCRYPTION_PASSWORD, properties.getProperty(SecurityConstants.REPO_ENCRYPTION_PASSWORD));
      passwordProperties.put(SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR, properties.getProperty(SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR));
      String password = PasswordUtils.readPassword(new MapContext(passwordProperties), SecurityConstants.REPO_ENCRYPTION_PASSWORD, SecurityConstants.REPO_ENCRYPTION_PASSWORD_GENERATOR);


      masterKeyManager.initialize(true, hmacAlgorithm, cipherAlgorithm, cipherSpec, cipherKeySize, ivLength, pbkdf2Algorithm, pbkdf2Rounds, password, readFromRepository, transaction);
    }

    return masterKeyManager;
  }
}
