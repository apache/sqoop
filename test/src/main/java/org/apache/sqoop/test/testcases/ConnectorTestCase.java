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
package org.apache.sqoop.test.testcases;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotSame;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.test.asserts.ProviderAsserts;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.test.data.Cities;
import org.apache.sqoop.test.data.ShortStories;
import org.apache.sqoop.test.data.UbuntuReleases;
import org.apache.sqoop.test.hadoop.HadoopMiniClusterRunner;
import org.apache.sqoop.test.hadoop.HadoopRunnerFactory;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

/**
 * Base test case suitable for connector testing.
 *
 * In addition to pure Tomcat based test case it will also create and initialize
 * the database provider prior every test execution.
 */
abstract public class ConnectorTestCase extends TomcatTestCase {

  private static final Logger LOG = Logger.getLogger(ConnectorTestCase.class);

  protected static DatabaseProvider provider;

  /**
   * Default submission callbacks that are printing various status about the submission.
   */
  protected static SubmissionCallback DEFAULT_SUBMISSION_CALLBACKS = new SubmissionCallback() {
    @Override
    public void submitted(MSubmission submission) {
      LOG.info("Submission submitted: " + submission);
    }

    @Override
    public void updated(MSubmission submission) {
      LOG.info("Submission updated: " + submission);
    }

    @Override
    public void finished(MSubmission submission) {
      LOG.info("Submission finished: " + submission);
    }
  };

  @BeforeSuite(alwaysRun = true)
  public static void startHadoop() throws Exception {
    // Start Hadoop Clusters
    hadoopCluster = HadoopRunnerFactory.getHadoopCluster(System.getProperties(), HadoopMiniClusterRunner.class);
    hadoopCluster.setTemporaryPath(TMP_PATH_BASE);
    hadoopCluster.setConfiguration( hadoopCluster.prepareConfiguration(new JobConf()) );
    hadoopCluster.start();

    // Initialize Hdfs Client
    hdfsClient = FileSystem.get(hadoopCluster.getConfiguration());
    LOG.debug("HDFS Client: " + hdfsClient);
  }

  @BeforeClass(alwaysRun = true)
  public static void startProvider() throws Exception {
    provider = DatabaseProviderFactory.getProvider(System.getProperties());
    LOG.info("Starting database provider: " + provider.getClass().getName());
    provider.start();
  }

  @AfterClass(alwaysRun = true)
  public static void stopProvider() {
    LOG.info("Stopping database provider: " + provider.getClass().getName());
    provider.stop();
  }

  public TableName getTableName() {
    return new TableName(getClass().getSimpleName());
  }

  protected void createTable(String primaryKey, String ...columns) {
    provider.createTable(getTableName(), primaryKey, columns);
  }

  protected void dropTable() {
    provider.dropTable(getTableName());
  }

  protected void insertRow(Object ...values) {
    provider.insertRow(getTableName(), values);
  }

  protected void insertRow(Boolean escapeValues, Object ...values) {
    provider.insertRow(getTableName(), escapeValues, values);
  }

  protected long rowCount() {
    return provider.rowCount(getTableName());
  }

  protected void dumpTable() {
    provider.dumpTable(getTableName());
  }

  /**
   * Fill link config based on currently active provider.
   *
   * @param link MLink object to fill
   */
  protected void fillRdbmsLinkConfig(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.jdbcDriver").setValue(provider.getJdbcDriver());
    configs.getStringInput("linkConfig.connectionString").setValue(provider.getConnectionUrl());
    configs.getStringInput("linkConfig.username").setValue(provider.getConnectionUsername());
    configs.getStringInput("linkConfig.password").setValue(provider.getConnectionPassword());
  }

  protected void fillRdbmsFromConfig(MJob job, String partitionColumn) {
    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.tableName").setValue(provider.escapeTableName(getTableName().getTableName()));
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue(provider.escapeColumnName(partitionColumn));
  }

  protected void fillRdbmsToConfig(MJob job) {
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.tableName").setValue(provider.escapeTableName(getTableName().getTableName()));
  }

  protected void fillHdfsLink(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.confDir").setValue(getCluster().getConfigurationPath());
  }

  /**
   * Fill TO config with specific storage and output type.
   *
   * @param job MJob object to fill
   * @param output Output type that should be set
   */
  protected void fillHdfsToConfig(MJob job, ToFormat output) {
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getEnumInput("toJobConfig.outputFormat").setValue(output);
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue(getMapreduceDirectory());
  }

  /**
   * Fill FROM config
   *
   * @param job MJob object to fill
   */
  protected void fillHdfsFromConfig(MJob job) {
    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.inputDirectory").setValue(getMapreduceDirectory());
  }

  /**
   * Fill Driver config
   * @param job
   */
  protected void fillDriverConfig(MJob job) {
    job.getDriverConfig().getStringInput("throttlingConfig.numExtractors").setValue("3");
  }


  /**
   * Create table cities.
   */
  protected void createTableCities() {
    new Cities(provider, getTableName()).createTables();
  }

  /**
   * Create table cities and load few rows.
   */
  protected void createAndLoadTableCities() {
    new Cities(provider, getTableName()).createTables().loadBasicData();
  }

  /**
   * Create table for ubuntu releases.
   */
  protected void createTableUbuntuReleases() {
    new UbuntuReleases(provider, getTableName()).createTables();
  }

  /**
   * Create table for ubuntu releases.
   */
  protected void createAndLoadTableUbuntuReleases() {
    new UbuntuReleases(provider, getTableName()).createTables().loadBasicData();
  }

  /**
   * Create table for short stories.
   */
  protected void createTableShortStories() {
    new ShortStories(provider, getTableName()).createTables();
  }

  /**
   * Create table for short stories.
   */
  protected void createAndLoadTableShortStories() {
    new ShortStories(provider, getTableName()).createTables().loadBasicData();
  }

  /**
   * Assert row in testing table.
   *
   * @param conditions Conditions in config that are expected by the database provider
   * @param values Values that are expected in the table (with corresponding types)
   */
  protected void assertRow(Object[] conditions, Object ...values) {
    ProviderAsserts.assertRow(provider, getTableName(), conditions, values);
  }

  /**
   * Assert row in testing table.
   *
   * @param conditions Conditions in config that are expected by the database provider
   * @param escapeValues Flag whether the values should be escaped based on their type when using in the generated queries or not
   * @param values Values that are expected in the table (with corresponding types)
   */
  protected void assertRow(Object []conditions, Boolean escapeValues, Object ...values) {
    ProviderAsserts.assertRow(provider, getTableName(), escapeValues, conditions, values);
  }

  /**
   * Assert row in table "cities".
   *
   * @param values Values that are expected
   */
  protected void assertRowInCities(Object... values) {
    assertRow(new Object[]{"id", values[0]}, values);
  }

  /**
   * Create link.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param link
   */
  protected void saveLink(MLink link) {
    assertEquals(Status.OK, getClient().saveLink(link));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, link.getPersistenceId());
  }

 /**
   * Create job.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param job
   */
 protected void saveJob(MJob job) {
    assertEquals(Status.OK, getClient().saveJob(job));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, job.getPersistenceId());
  }

  /**
   * Run job with given jid.
   *
   * @param jid Job id
   * @throws Exception
   */
  protected void executeJob(long jid) throws Exception {
    MSubmission finalSubmission = getClient().startJob(jid, DEFAULT_SUBMISSION_CALLBACKS, 100);

    if(finalSubmission.getStatus().isFailure()) {
      LOG.error("Submission has failed: " + finalSubmission.getError().getErrorSummary());
      LOG.error("Corresponding error details: " + finalSubmission.getError().getErrorDetails());
    }
    assertEquals("Submission finished with error: " + finalSubmission.getError().getErrorSummary(), SubmissionStatus.SUCCEEDED, finalSubmission.getStatus());
  }

  /**
   * Run given job.
   *
   * @param job Job object
   * @throws Exception
   */
  protected void executeJob(MJob job) throws Exception {
    executeJob(job.getPersistenceId());
  }
}
