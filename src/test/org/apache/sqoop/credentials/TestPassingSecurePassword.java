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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
      fail("The password file does not exist! ");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("The password file does not exist!"));
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
      fail("The password file cannot be a directory! ");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("The password file cannot "
        + "be a directory!"));
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
      fail("Either password or passwordPath must be specified but not both.");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Either password or path to a "
        + "password file must be specified but not both"));
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

  private void createTempFile(String filePath) throws IOException {
    File pwdFile = new File(filePath);
    pwdFile.createNewFile();
  }

  private void writeToFile(String filePath, String contents)
    throws IOException {
    File pwdFile = new File(filePath);
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(pwdFile);
      fos.write(contents.getBytes());
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }
}
