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
package org.apache.sqoop.test.infrastructure;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.common.test.db.DatabaseProvider;
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
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.InfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.test.utils.SqoopUtils;
import org.apache.sqoop.validation.Status;
import org.testng.ITest;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

/**
 * Use Infrastructure annotation to boot up miniclusters.
 * Order is built-in to code. Hadoop comes first, then
 * the rest of the services.
 */
public class SqoopTestCase implements ITest {
  private static final Logger LOG = Logger.getLogger(SqoopTestCase.class);

  private static final String ROOT_PATH = System.getProperty("sqoop.integration.tmpdir", System.getProperty("java.io.tmpdir", "/tmp")) + "/sqoop-cargo-tests";

  private static final Map<String, InfrastructureProvider> PROVIDERS
      = new HashMap<String, InfrastructureProvider>();

  /**
   * Default submission callbacks that are printing various status about the submission.
   */
  protected static final SubmissionCallback DEFAULT_SUBMISSION_CALLBACKS = new SubmissionCallback() {
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

  private static String suiteName;

  private String methodName;

  private SqoopClient client;

  @BeforeSuite
  public static void findSuiteName(ITestContext context) {
    suiteName = context.getSuite().getName();
  }

  @BeforeMethod
  public void findMethodName(Method method) {
    methodName = method.getName();
  }

  @Override
  public String getTestName() {
    return methodName;
  }

  /**
   * Create infrastructure components and start those services.
   * @param context TestNG context that helps get all the test methods and classes.
   */
  @BeforeSuite(dependsOnMethods = "findSuiteName")
  public static void startInfrastructureProviders(ITestContext context) {
    // Find infrastructure provider classes to be used.
    Set<Class<? extends InfrastructureProvider>> providers = new HashSet<Class<? extends InfrastructureProvider>>();
    for (ITestNGMethod method : context.getSuite().getAllMethods()) {
      LOG.debug("Looking up dependencies on method ("
          + method.getConstructorOrMethod().getDeclaringClass().getCanonicalName()
          + "#" + method.getConstructorOrMethod().getMethod().getName()
          + ")");
      Infrastructure ann;

      // If the method has an infrastructure annotation, process it.
      if (method.getConstructorOrMethod().getMethod() != null) {
        ann = method.getConstructorOrMethod().getMethod().getAnnotation(Infrastructure.class);
        if (ann != null && ann.dependencies() != null) {
          LOG.debug("Found dependencies on method ("
              + method.getConstructorOrMethod().getDeclaringClass().getCanonicalName()
              + "#" + method.getConstructorOrMethod().getMethod().getName()
              + "): " + StringUtils.join(ann.dependencies(), ","));
          providers.addAll(Arrays.asList(ann.dependencies()));
        }
      }

      // Declaring class should be processed always.
      ann = method.getConstructorOrMethod().getDeclaringClass().getAnnotation(Infrastructure.class);
      if (ann != null && ann.dependencies() != null) {
        LOG.debug("Found dependencies on class ("
            + method.getConstructorOrMethod().getDeclaringClass().getCanonicalName()
            + "): " + StringUtils.join(ann.dependencies(), ","));
        providers.addAll(Arrays.asList(ann.dependencies()));
      }
    }

    // Create/start infrastructure providers.
    Configuration conf = new JobConf();

    // Start hadoop first.
    if (providers.contains(HadoopInfrastructureProvider.class)) {
      InfrastructureProvider hadoopProviderObject = startInfrastructureProvider(HadoopInfrastructureProvider.class, conf);

      // Use the prepared hadoop configuration for the rest of the components.
      if (hadoopProviderObject != null) {
        conf = hadoopProviderObject.getHadoopConfiguration();
      }
    }

    // Start the rest of the providers.
    for (Class<? extends InfrastructureProvider> provider : providers) {
      startInfrastructureProvider(provider, conf);
    }
  }

  /**
   * Start an infrastructure provider and add it to the PROVIDERS map
   * for stopping in the future.
   * @param providerClass
   * @param hadoopConfiguration
   * @param <T>
   * @return
   */
  private static <T extends InfrastructureProvider> T startInfrastructureProvider(Class<T> providerClass, Configuration hadoopConfiguration) {
    T providerObject;

    try {
      providerObject = providerClass.newInstance();
    } catch (Exception e) {
      LOG.error("Could not instantiate new instance of InfrastructureProvider.", e);
      return null;
    }

    providerObject.setRootPath(HdfsUtils.joinPathFragments(ROOT_PATH, suiteName, providerClass.getCanonicalName()));
    providerObject.setHadoopConfiguration(hadoopConfiguration);
    providerObject.start();

    // Add for recall later.
    PROVIDERS.put(providerClass.getCanonicalName(), providerObject);

    System.out.println("Infrastructure Provider " + providerClass.getCanonicalName());

    return providerObject;
  }

  /**
   * Stop infrastructure components and services.
   */
  @AfterSuite
  public static void stopInfrastructureProviders() {
    // Hadoop infrastructure provider included in PROVIDERS.
    for (InfrastructureProvider provider : PROVIDERS.values()) {
      provider.stop();
    }
  }

  /**
   * Get the infrastructure provider from the PROVIDERS map.
   * @param providerClass
   * @param <T>
   * @return T InfrastructureProvider
   */
  public static <T extends InfrastructureProvider> T getInfrastructureProvider(Class<T> providerClass) {
    InfrastructureProvider provider = PROVIDERS.get(providerClass.getCanonicalName());
    return ((T) provider);
  }

  /**
   * Get the data directory for tests.
   * @return
   */
  public String getMapreduceDirectory() {
    return HdfsUtils.joinPathFragments(
        getInfrastructureProvider(HadoopInfrastructureProvider.class).getInstance().getTestDirectory(),
        getClass().getName(),
        getTestName());
  }

  /**
   * Fill RDBMS Link Configuration with infrastructure provider info.
   * @param link
   */
  public void fillRdbmsLinkConfig(MLink link) {
    DatabaseProvider provider = getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance();

    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.jdbcDriver").setValue(provider.getJdbcDriver());
    configs.getStringInput("linkConfig.connectionString").setValue(provider.getConnectionUrl());
    configs.getStringInput("linkConfig.username").setValue(provider.getConnectionUsername());
    configs.getStringInput("linkConfig.password").setValue(provider.getConnectionPassword());
  }

  /**
   * Fill RDBMS FROM Configuration with infrastructure provider info.
   * @param job
   * @param partitionColumn
   */
  public void fillRdbmsFromConfig(MJob job, String partitionColumn) {
    DatabaseProvider provider = getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance();

    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.tableName").setValue(provider.escapeTableName(getTableName().getTableName()));
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue(provider.escapeColumnName(partitionColumn));
  }

  /**
   * Fill RDBMS TO Configuration with infrastructure provider info.
   * @param job
   */
  public void fillRdbmsToConfig(MJob job) {
    DatabaseProvider provider = getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance();

    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.tableName").setValue(provider.escapeTableName(getTableName().getTableName()));
  }

  /**
   * Fill HDFS Link Configuration with infrastructure provider info.
   * @param link
   */
  public void fillHdfsLinkConfig(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.confDir").setValue(
        getInfrastructureProvider(SqoopInfrastructureProvider.class).getInstance()
            .getConfigurationPath());
  }

  /**
   * Fill HDFS FROM Configuration with infrastructure provider info.
   * @param job
   */
  public void fillHdfsFromConfig(MJob job) {
    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.inputDirectory").setValue(getMapreduceDirectory());
  }

  /**
   * Fill HDFS TO Configuration with infrastructure provider info.
   * @param job
   * @param output
   */
  public void fillHdfsToConfig(MJob job, ToFormat output) {
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getEnumInput("toJobConfig.outputFormat").setValue(output);
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue(getMapreduceDirectory());
  }

  public String getSqoopServerUrl() {
    if (getInfrastructureProvider(SqoopInfrastructureProvider.class) == null) {
      return null;
    }

    return getInfrastructureProvider(SqoopInfrastructureProvider.class).getInstance()
        .getServerUrl();
  }

  /**
   * Create a sqoop client
   * @return SqoopClient
   */
  public SqoopClient getClient() {
    if (client == null) {
      String serverUrl = getSqoopServerUrl();

      if (serverUrl != null) {
        client = new SqoopClient(serverUrl);
      }
    }
    return client;
  }

  /**
   * Create link with asserts to make sure that it was created correctly.
   *
   * @param link
   */
  public void saveLink(MLink link) {
    SqoopUtils.fillObjectName(link);
    assertEquals(Status.OK, getClient().saveLink(link));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, link.getPersistenceId());
  }

  /**
   * Create job with asserts to make sure that it was created correctly.
   *
   * @param job
   */
  public void saveJob(MJob job) {
    SqoopUtils.fillObjectName(job);
    assertEquals(Status.OK, getClient().saveJob(job));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, job.getPersistenceId());
  }

  /**
   * Run job with given jid.
   *
   * @param jid Job id
   * @throws Exception
   */
  public void executeJob(long jid) throws Exception {
    MSubmission finalSubmission = getClient().startJob(jid, DEFAULT_SUBMISSION_CALLBACKS, 100);

    if(finalSubmission.getStatus().isFailure()) {
      LOG.error("Submission has failed: " + finalSubmission.getError().getErrorSummary());
      LOG.error("Corresponding error details: " + finalSubmission.getError().getErrorDetails());
    }
    assertEquals(SubmissionStatus.SUCCEEDED, finalSubmission.getStatus(), "Submission finished with error: " + finalSubmission.getError().getErrorSummary());
  }

  /**
   * Fetch table name to be used by this test.
   * @return TableName
   */
  public TableName getTableName() {
    return new TableName(getClass().getSimpleName());
  }

  /**
   * Create table with table name for this test.
   * @param primaryKey
   * @param columns
   */
  public void createTable(String primaryKey, String ...columns) {
    getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .createTable(getTableName(), primaryKey, columns);
  }

  /**
   * Drop table for this test.
   */
  public void dropTable() {
    getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .dropTable(getTableName());
  }

  /**
   * Insert row into table for this test.
   * @param values
   */
  public void insertRow(Object ...values) {
    getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .insertRow(getTableName(), values);
  }

  /**
   * Insert row into table for this test.
   * @param escapeValues
   * @param values
   */
  public void insertRow(Boolean escapeValues, Object ...values) {
    getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .insertRow(getTableName(), escapeValues, values);
  }

  /**
   * Fetch row count of table for this test.
   * @return long count
   */
  public long rowCount() {
    return getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .rowCount(getTableName());
  }

  /**
   * Dump the table for this test.
   */
  public void dumpTable() {
    getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance()
        .dumpTable(getTableName());
  }

  /**
   * Create and load cities data.
   */
  public void createAndLoadTableCities() {
    new Cities(getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance(), getTableName()).createTables().loadBasicData();
  }

  /**
   * Create ubuntu releases table.
   */
  public void createTableUbuntuReleases() {
    new UbuntuReleases(getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance(), getTableName()).createTables();
  }

  /**
   * Create and load ubuntu releases data.
   */
  public void createAndLoadTableUbuntuReleases() {
    new UbuntuReleases(getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance(), getTableName()).createTables().loadBasicData();
  }

  /**
   * Create short stories table.
   */
  public void createTableShortStories() {
    new ShortStories(getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance(), getTableName()).createTables();
  }

  /**
   * Create and load short stories data.
   */
  public void createAndLoadTableShortStories() {
    new ShortStories(getInfrastructureProvider(DatabaseInfrastructureProvider.class).getInstance(), getTableName()).createTables().loadBasicData();
  }
}
