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

package org.apache.sqoop.credentials;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.tool.BaseSqoopTool;
import org.apache.sqoop.tool.ImportTool;
import org.apache.sqoop.util.password.CredentialProviderHelper;
import org.apache.sqoop.util.password.CredentialProviderPasswordLoader;
import org.apache.sqoop.util.password.CryptoFileLoader;
import org.apache.sqoop.util.password.PasswordLoader;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * Set of tests for securing passwords.
 */
public class TestPassingSecurePassword extends BaseSqoopTestCase {

  @Override
  public void setUp() {
    super.setUp();
    Path warehousePath = new Path(this.getWarehouseDir());
    try {
      FileSystem fs = FileSystem.get(getConf());
      fs.create(warehousePath, true);
    } catch (IOException e) {
      System.out.println("Could not create warehouse dir!");
    }
  }

  public void testPasswordFilePathInOptionIsEnabled() throws Exception {
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);

    try {
      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--username");
      extraArgs.add("username");
      extraArgs.add("--password-file");
      extraArgs.add(passwordFilePath);
      String[] commonArgs = getCommonArgs(false, extraArgs);
      ArrayList<String> argsList = new ArrayList<String>();
      Collections.addAll(argsList, commonArgs);
      assertTrue("passwordFilePath option missing.",
        argsList.contains("--password-file"));
    } catch (Exception e) {
      fail("passwordPath option is missing.");
    }
  }

  public void testPasswordFileDoesNotExist() throws Exception {
    try {
      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--password-file");
      extraArgs.add(TEMP_BASE_DIR + "unknown");
      String[] argv = getCommonArgs(false, extraArgs);

      Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();
      importTool.parseArguments(argv, conf, opts, true);
      fail("The password file does not exist!");
    } catch (Exception e) {
      assertTrue(e.getMessage().matches(".*The provided password file "
        + ".* does not exist!"));
    }
  }

  public void testPasswordFileIsADirectory() throws Exception {
    try {
      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--password-file");
      extraArgs.add(TEMP_BASE_DIR);
      String[] argv = getCommonArgs(false, extraArgs);

      Configuration conf = getConf();
      SqoopOptions opts = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();
      importTool.parseArguments(argv, conf, opts, true);
      fail("The password file cannot be a directory!");
    } catch (Exception e) {
      assertTrue(e.getMessage().matches(".*The provided password file .*"
        + " is a directory!"));
    }
  }

  public void testBothPasswordOptions() throws Exception {
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);

    try {
      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--username");
      extraArgs.add("username");
      extraArgs.add("--password");
      extraArgs.add("password");
      extraArgs.add("--password-file");
      extraArgs.add(passwordFilePath);
      String[] argv = getCommonArgs(false, extraArgs);

      Configuration conf = getConf();
      SqoopOptions in = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();
      SqoopOptions out = importTool.parseArguments(argv, conf, in, true);
      assertNotNull(out.getPassword());
      importTool.validateOptions(out);
      fail("Only one of password, password "
          + "alias or path to a password file must be specified.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Only one of password, password "
          + "alias or path to a password file must be specified."));
    }
  }

  public void testPasswordFilePath() throws Exception {
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);
    writeToFile(passwordFilePath, "password");

    try {
      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--username");
      extraArgs.add("username");
      extraArgs.add("--password-file");
      extraArgs.add(passwordFilePath);
      String[] commonArgs = getCommonArgs(false, extraArgs);

      Configuration conf = getConf();
      SqoopOptions in = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();
      SqoopOptions out = importTool.parseArguments(commonArgs, conf, in, true);
      assertNotNull(out.getPasswordFilePath());
      assertNotNull(out.getPassword());
      assertEquals("password", out.getPassword());
    } catch (Exception e) {
      fail("passwordPath option is missing.");
    }
  }

  public void testPasswordInDBConfiguration() throws Exception {
    JobConf jobConf = new JobConf(getConf());
    DBConfiguration.configureDB(jobConf, "org.hsqldb.jdbcDriver",
      getConnectString(), "username", "password", null, null);

    assertNotNull(jobConf.getCredentials().getSecretKey(
      new Text(DBConfiguration.PASSWORD_PROPERTY)));
    assertEquals("password", new String(jobConf.getCredentials().getSecretKey(
      new Text(DBConfiguration.PASSWORD_PROPERTY))));

    // necessary to wipe the state of previous call to configureDB
    jobConf = new JobConf();
    DBConfiguration.configureDB(jobConf, "org.hsqldb.jdbcDriver",
      getConnectString(), null, null, null, null);
    DBConfiguration dbConfiguration = new DBConfiguration(jobConf);
    Connection connection = dbConfiguration.getConnection();
    assertNotNull(connection);
  }

  public void testPasswordNotInJobConf() throws Exception {
    JobConf jobConf = new JobConf(getConf());
    DBConfiguration.configureDB(jobConf, "org.hsqldb.jdbcDriver",
      getConnectString(), "username", "password", null, null);

    assertNull(jobConf.get(DBConfiguration.PASSWORD_PROPERTY, null));
  }

  public void testPasswordInMetastoreWithRecordEnabledAndSecureOption()
    throws Exception {
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);

    ArrayList<String> extraArgs = new ArrayList<String>();
    extraArgs.add("--username");
    extraArgs.add("username");
    extraArgs.add("--password-file");
    extraArgs.add(passwordFilePath);
    String[] argv = getCommonArgs(false, extraArgs);

    Configuration conf = getConf();
    SqoopOptions in = getSqoopOptions(conf);
    ImportTool importTool = new ImportTool();
    SqoopOptions out = importTool.parseArguments(argv, conf, in, true);
    assertNotNull(out.getPassword());

    // Enable storing passwords in the metastore
    conf.set(SqoopOptions.METASTORE_PASSWORD_KEY, "true");

    // this is what is used to record password into the metastore
    Properties propertiesIntoMetastore = out.writeProperties();

    assertNull(propertiesIntoMetastore.getProperty("db.password"));
    // password-file should NOT be null as it'll be sued to retrieve password
    assertNotNull(propertiesIntoMetastore.getProperty("db.password.file"));

    // load the saved properties and verify
    SqoopOptions optionsFromMetastore = new SqoopOptions();
    optionsFromMetastore.loadProperties(propertiesIntoMetastore);
    assertNotNull(optionsFromMetastore.getPassword());
    assertNotNull(optionsFromMetastore.getPasswordFilePath());
    assertEquals(passwordFilePath, optionsFromMetastore.getPasswordFilePath());
  }

  public void testPasswordInMetastoreWithRecordDisabledAndSecureOption()
    throws Exception {
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);

    ArrayList<String> extraArgs = new ArrayList<String>();
    extraArgs.add("--username");
    extraArgs.add("username");
    extraArgs.add("--password-file");
    extraArgs.add(passwordFilePath);
    String[] argv = getCommonArgs(false, extraArgs);

    Configuration conf = getConf();
    SqoopOptions in = getSqoopOptions(conf);
    ImportTool importTool = new ImportTool();
    SqoopOptions out = importTool.parseArguments(argv, conf, in, true);
    assertNotNull(out.getPassword());

    // Enable storing passwords in the metastore
    conf.set(SqoopOptions.METASTORE_PASSWORD_KEY, "false");

    // this is what is used to record password into the metastore
    Properties propertiesIntoMetastore = out.writeProperties();

    assertNull(propertiesIntoMetastore.getProperty("db.password"));
    assertNotNull(propertiesIntoMetastore.getProperty("db.password.file"));

    // load the saved properties and verify
    SqoopOptions optionsFromMetastore = new SqoopOptions();
    optionsFromMetastore.loadProperties(propertiesIntoMetastore);
    assertNotNull(optionsFromMetastore.getPassword());
    assertNotNull(optionsFromMetastore.getPasswordFilePath());
    assertEquals(passwordFilePath, optionsFromMetastore.getPasswordFilePath());
  }

  public void testPasswordInMetastoreWithRecordEnabledAndNonSecureOption()
    throws Exception {
    ArrayList<String> extraArgs = new ArrayList<String>();
    extraArgs.add("--username");
    extraArgs.add("username");
    extraArgs.add("--password");
    extraArgs.add("password");
    String[] argv = getCommonArgs(false, extraArgs);

    Configuration conf = getConf();
    SqoopOptions in = getSqoopOptions(conf);
    ImportTool importTool = new ImportTool();
    SqoopOptions out = importTool.parseArguments(argv, conf, in, true);
    assertNotNull(out.getPassword());

    // Enable storing passwords in the metastore
    conf.set(SqoopOptions.METASTORE_PASSWORD_KEY, "true");

    // this is what is used to record password into the metastore
    Properties propertiesIntoMetastore = out.writeProperties();

    assertNotNull(propertiesIntoMetastore.getProperty("db.password"));
    assertNull(propertiesIntoMetastore.getProperty("db.password.file"));

    // load the saved properties and verify
    SqoopOptions optionsFromMetastore = new SqoopOptions();
    optionsFromMetastore.loadProperties(propertiesIntoMetastore);
    assertNotNull(optionsFromMetastore.getPassword());
    assertNull(optionsFromMetastore.getPasswordFilePath());
  }

  private String[] getCommonArgs(boolean includeHadoopFlags,
                                 ArrayList<String> extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("--as-textfile");
    args.add("--num-mappers");
    args.add("2");

    args.addAll(extraArgs);

    return args.toArray(new String[0]);
  }

  public void testCryptoFileLoader() throws Exception {
    // Current implementation is limited to ECB mode
    Object[][] ciphers = {
//      {"AES/CBC/NoPadding", 128},
//      {"AES/CBC/PKCS5Padding", 128},
      {"AES/ECB/NoPadding", 128},
      {"AES/ECB/PKCS5Padding", 128},
//      {"DES/CBC/NoPadding", 56},
//      {"DES/CBC/PKCS5Padding", 56},
      {"DES/ECB/NoPadding", 64},
      {"DES/ECB/PKCS5Padding", 64},
//      {"DESede/CBC/NoPadding", 168},
//      {"DESede/CBC/PKCS5Padding", 168},
      {"DESede/ECB/NoPadding", 192},
      {"DESede/ECB/PKCS5Padding", 192}
    };

    String [] passphrases = {
      "Simple  password",
      "!@#$%^&*()_+<>?:"
    };

    // Execute all ciphers with all pass phrases
    for(Object [] cipher : ciphers) {
      for(String pass : passphrases) {
        executeCipherTest(pass, pass, (String)cipher[0], (Integer)cipher[1]);
      }
    }
  }

  public void testCredentialProviderLoader() throws Exception {
    CredentialProviderPasswordLoader pl =
        new CredentialProviderPasswordLoader();

    if (!CredentialProviderHelper.isProviderAvailable()) {
      LOG.info("CredentialProvider facility not available "
        + "in the hadoop environment used");
    } else {
      String alias = "super.secret.alias";
      String pw = "super.secret.password";

      String passwordFilePath = TEMP_BASE_DIR + ".pwd";
      String jksFile = "creds.jks";
      createTempFile(passwordFilePath);
      writeToFile(passwordFilePath, alias.getBytes());
      File credDir = new File(".");

      Configuration conf = getConf();
      String ourUrl =  CredentialProviderHelper.SCHEME_NAME +
        "://file/" + credDir.getAbsolutePath() + "/" + jksFile;
      File file = new File(credDir, jksFile);
      file.delete();
      conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH,
        ourUrl);
      CredentialProviderHelper.createCredentialEntry(conf, alias, pw);

      conf.set("org.apache.sqoop.credentials.loader.class",
        CredentialProviderPasswordLoader.class.getCanonicalName());

      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--username");
      extraArgs.add("username");
      extraArgs.add("--password-file");
      extraArgs.add(passwordFilePath);
      String[] commonArgs = getCommonArgs(false, extraArgs);

      SqoopOptions in = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();

      SqoopOptions out = importTool.parseArguments(commonArgs, conf, in, true);
      assertEquals(pw, pl.loadPassword(passwordFilePath, conf));
      assertEquals(pw, out.getPassword());
    }
  }

  public void testPasswordAliasOption() throws Exception {
    CredentialProviderPasswordLoader pl =
        new CredentialProviderPasswordLoader();

    if (!CredentialProviderHelper.isProviderAvailable()) {
      LOG.info("CredentialProvider facility not available "
        + "in the hadoop environment used");
    } else {
      String alias = "super.secret.alias";
      String pw = "super.secret.password";
      String jksFile = "creds.jks";
      File credDir = new File(".");

      Configuration conf = getConf();
      String ourUrl =  CredentialProviderHelper.SCHEME_NAME +
        "://file/" + credDir.getAbsolutePath() + "/" + jksFile;
      File file = new File(credDir, jksFile);
      file.delete();
      conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH,
        ourUrl);
      CredentialProviderHelper.createCredentialEntry(conf, alias, pw);

      ArrayList<String> extraArgs = new ArrayList<String>();
      extraArgs.add("--username");
      extraArgs.add("username");
      extraArgs.add("--password-alias");
      extraArgs.add(alias);
      String[] commonArgs = getCommonArgs(false, extraArgs);

      SqoopOptions in = getSqoopOptions(conf);
      ImportTool importTool = new ImportTool();

      SqoopOptions out = importTool.parseArguments(commonArgs, conf, in, true);
      assertEquals(pw, out.getPassword());
    }
  }

  public void executeCipherTest(String password, String passphrase, String cipher, int keySize) throws Exception {
    LOG.info("Using cipher: " + cipher + " with keySize " + keySize + " and passphrase " + passphrase );
    String passwordFilePath = TEMP_BASE_DIR + ".pwd";
    createTempFile(passwordFilePath);
    writeToFile(passwordFilePath, encryptPassword(password, passphrase, cipher, 10000, keySize));
    LOG.info("Generated encrypted password file in: " + passwordFilePath);

    ArrayList<String> extraArgs = new ArrayList<String>();
    extraArgs.add("--username");
    extraArgs.add("username");
    extraArgs.add("--password-file");
    extraArgs.add(passwordFilePath);
    String[] commonArgs = getCommonArgs(false, extraArgs);

    Configuration conf = getConf();
    conf.set("org.apache.sqoop.credentials.loader.class", CryptoFileLoader.class.getCanonicalName());
    conf.set("org.apache.sqoop.credentials.loader.crypto.alg", cipher);
    conf.set("org.apache.sqoop.credentials.loader.crypto.passphrase", passphrase);
    conf.setInt("org.apache.sqoop.credentials.loader.crypto.salt.key.len", keySize);

    SqoopOptions in = getSqoopOptions(conf);
    ImportTool importTool = new ImportTool();

    SqoopOptions out = importTool.parseArguments(commonArgs, conf, in, true);
    assertNotNull(out.getPasswordFilePath());
    assertNotNull(out.getPassword());
    assertEquals(passphrase, out.getPassword());
  }

  private byte[] encryptPassword(String password, String passPhrase, String alg, int iterations, int keySize) throws Exception {
    String algOnly = alg.split("/")[0];

    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    SecretKey secKey =  factory.generateSecret(new PBEKeySpec(passPhrase.toCharArray(), "SALT".getBytes(), iterations, keySize));
    SecretKeySpec key = new SecretKeySpec(secKey.getEncoded(), algOnly);

    Cipher crypto = Cipher.getInstance(alg);
    crypto.init(Cipher.ENCRYPT_MODE, key);

    return crypto.doFinal(password.getBytes());
  }

  private void createTempFile(String filePath) throws IOException {
    File pwdFile = new File(filePath);
    pwdFile.createNewFile();
  }

  private void writeToFile(String filePath, String contents) throws IOException {
    writeToFile(filePath, contents.getBytes());
  }

  private void writeToFile(String filePath, byte [] contents) throws IOException {
    File pwdFile = new File(filePath);
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(pwdFile);
      fos.write(contents);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }
}
