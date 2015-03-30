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
package org.apache.sqoop.integration.connector.jdbc.generic;

import com.google.common.collect.Iterables;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;
import org.apache.sqoop.common.test.db.types.DatabaseType;
import org.apache.sqoop.common.test.db.types.ExampleValue;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.test.utils.ParametrizedUtils;
import org.testng.ITest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;

/**
 * Test transfer of all supported data types.
 */
public class AllTypesTest extends ConnectorTestCase implements ITest {

  @DataProvider(name="all-types-test", parallel=true)
  public static Object[][] data() throws Exception {
    DatabaseProvider provider = DatabaseProviderFactory.getProvider(System.getProperties());
    return Iterables.toArray(ParametrizedUtils.toArrayOfArrays(provider.getDatabaseTypes().getAllTypes()), Object[].class);
  }

  @Factory(dataProvider="all-types-test")
  public AllTypesTest(DatabaseType type) {
    this.type = type;
  }

  private DatabaseType type;

  @Test
  public void testFrom() throws Exception {
    createTable("id",
      "id", "INT",
      "value", type.name
    );

    int i = 1;
    for(ExampleValue value: type.values) {
      insertRow(false, Integer.toString(i++), value.insertStatement);
    }

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Fill rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");
    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.columns").setValue(provider.escapeColumnName("value"));

    // Fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

    saveJob(job);
    executeJob(job);

    // Assert correct output
    assertTo(type.escapedStringValues());

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testTo() throws Exception {
    createTable(null,
      "value", type.name
    );

    createFromFile("input-0001", type.escapedStringValues());

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    fillHdfsFromConfig(job);

    // Set the rdbms "TO" config here
    fillRdbmsToConfig(job);

    // Driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

    saveJob(job);
    executeJob(job);
    dumpTable();

    assertEquals(type.values.size(), rowCount());
    for(ExampleValue value : type.values) {
      assertRow(
        new Object[] {"value", value.insertStatement},
        false,
        value.objectValue);
    }

    // Clean up testing table
    dropTable();
  }

  private String testName;

  @BeforeMethod(alwaysRun = true)
  public void beforeMethod(Method aMethod) {
    this.testName = aMethod.getName();
  }

  @Override
  public String getTestName() {
    return testName + "[" + type.name + "]";
  }
}
