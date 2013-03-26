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
package org.apache.sqoop.integration.connector;

import org.apache.log4j.Logger;
import org.apache.sqoop.framework.configuration.OutputFormat;
import org.apache.sqoop.framework.configuration.StorageType;
import org.apache.sqoop.integration.TomcatTestCase;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.test.db.DatabaseProvider;
import org.apache.sqoop.test.db.DatabaseProviderFactory;
import org.apache.sqoop.validation.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Base test case for connector testing.
 *
 * It will create and initialize database provider prior every test execution.
 */
abstract public class ConnectorTestCase extends TomcatTestCase {

  private static final Logger LOG = Logger.getLogger(ConnectorTestCase.class);

  protected static DatabaseProvider provider;

  @BeforeClass
  public static void startProvider() throws Exception {
    provider = DatabaseProviderFactory.getProvider(System.getProperties());
    LOG.info("Starting database provider: " + provider.getClass().getName());
    provider.start();
  }

  @AfterClass
  public static void stopProvider() {
    LOG.info("Stopping database provider: " + provider.getClass().getName());
    provider.stop();
  }

  public String getTableName() {
    return getClass().getSimpleName();
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

  /**
   * Fill connection form based on currently active provider.
   *
   * @param connection MConnection object to fill
   */
  protected void fillConnectionForm(MConnection connection) {
    MFormList forms = connection.getConnectorPart();
    forms.getStringInput("connection.jdbcDriver").setValue(provider.getJdbcDriver());
    forms.getStringInput("connection.connectionString").setValue(provider.getConnectionUrl());
    forms.getStringInput("connection.username").setValue(provider.getConnectionUsername());
    forms.getStringInput("connection.password").setValue(provider.getConnectionPassword());
  }

  /**
   * Fill output form with specific storage and output type. Mapreduce output directory
   * will be set to default test value.
   *
   * @param job MJOb object to fill
   * @param storage Storage type that should be set
   * @param output Output type that should be set
   */
  protected void fillOutputForm(MJob job, StorageType storage, OutputFormat output) {
    MFormList forms = job.getFrameworkPart();
    forms.getEnumInput("output.storageType").setValue(storage);
    forms.getEnumInput("output.outputFormat").setValue(output);
    forms.getStringInput("output.outputDirectory").setValue(getMapreduceDirectory());
  }

  /**
   * Create table cities.
   */
  protected void createAndLoadTableCities() {
    createTable("id",
      "id", "int",
      "country", "varchar(50)",
      "city", "varchar(50)"
    );
    insertRow(1, "USA", "San Francisco");
    insertRow(2, "USA", "Sunnyvale");
    insertRow(3, "Czech Republic", "Brno");
    insertRow(4, "USA", "Palo Alto");
  }

  /**
   * Create connection.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param connection
   */
  protected void createConnection(MConnection connection) {
    assertEquals(Status.FINE, getClient().createConnection(connection));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, connection.getPersistenceId());
  }

 /**
   * Create job.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param job
   */
 protected void createJob(MJob job) {
    assertEquals(Status.FINE, getClient().createJob(job));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, job.getPersistenceId());
  }
}
