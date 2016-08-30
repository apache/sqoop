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

import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Import simple table with various configurations.
 */
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class FromRDBMSToHDFSTest extends SqoopTestCase {

  @Test
  public void testCities() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getName(), hdfsConnection.getName());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
      "1,'USA','2004-10-23 00:00:00.000','San Francisco'",
      "2,'USA','2004-10-24 00:00:00.000','Sunnyvale'",
      "3,'Czech Republic','2004-10-25 00:00:00.000','Brno'",
      "4,'USA','2004-10-26 00:00:00.000','Palo Alto'",
      "5,'USA','2004-10-27 00:00:00.000','Martha\\'s Vineyard'"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testStories() throws Exception {
    createAndLoadTableShortStories();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());

    // Connector values
    fillRdbmsFromConfig(job, "id");
    MConfigList configs = job.getFromJobConfig();
    List<String> columns = new java.util.LinkedList<>();
    columns.add("id");
    columns.add("name");
    columns.add("story");
    configs.getListInput("fromJobConfig.columnList").setValue(columns);
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
        "1,'The Gift of the Magi','ONE DOLLAR AND EIGHTY-SEVEN CENTS. THAT WAS ALL. AND SIXTY CENTS of it was in pennies. Pennies saved one and two at a time by bulldozing the grocer and the vegetable man and the butcher until ones cheeks burned with the silent imputation of parsimony that such close dealing implied. Three times Della counted it. One dollar and eighty-seven cents. And the next day would be Christmas.\\n\\nThere was clearly nothing left to do but flop down on the shabby little couch and howl. So Della did it. Which instigates the moral reflection that life is made up of sobs, sniffles, and smiles, with sniffles predominating.'",
        "2,'The Little Match Girl','Most terribly cold it was; it snowed, and was nearly quite dark, and evening-- the last evening of the year. In this cold and darkness there went along the street a poor little girl, bareheaded, and with naked feet. When she left home she had slippers on, it is true; but what was the good of that? They were very large slippers, which her mother had hitherto worn; so large were they; and the poor little thing lost them as she scuffled away across the street, because of two carriages that rolled by dreadfully fast.'",
        "3,'To Build a Fire','Day had broken cold and grey, exceedingly cold and grey, when the man turned aside from the main Yukon trail and climbed the high earth- bank, where a dim and little-travelled trail led eastward through the fat spruce timberland. It was a steep bank, and he paused for breath at the top, excusing the act to himself by looking at his watch. It was nine oclock. There was no sun nor hint of sun, though there was not a cloud in the sky. It was a clear day, and yet there seemed an intangible pall over the face of things, a subtle gloom that made the day dark, and that was due to the absence of sun. This fact did not worry the man. He was used to the lack of sun. It had been days since he had seen the sun, and he knew that a few more days must pass before that cheerful orb, due south, would just peep above the sky- line and dip immediately from view.'"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testColumns() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());

    // Connector values
    fillRdbmsFromConfig(job, "id");
    MConfigList configs = job.getFromJobConfig();
    List<String> columns = new java.util.LinkedList<>();
    columns.add("id");
    columns.add("country");
    configs.getListInput("fromJobConfig.columnList").setValue(columns);
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
      "1,'USA'",
      "2,'USA'",
      "3,'Czech Republic'",
      "4,'USA'",
      "5,'USA'"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testSql() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());

    // Connector values
    MConfigList configs = job.getFromJobConfig();
    configs.getStringInput("fromJobConfig.sql").setValue("SELECT " + provider.escapeColumnName("id")
        + " FROM " + provider.escapeTableName(getTableName().getTableName()) + " WHERE ${CONDITIONS}");
    configs.getStringInput("fromJobConfig.partitionColumn").setValue("id");
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
        "1",
        "2",
        "3",
        "4",
        "5"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testDuplicateColumns() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());

    // Connector values
    String partitionColumn = provider.escapeTableName(getTableName().getTableName()) + "." + provider.escapeColumnName("id");
    MConfigList configs = job.getFromJobConfig();
    configs.getStringInput("fromJobConfig.sql").setValue(
        "SELECT " + provider.escapeColumnName("id") + " as " + provider.escapeColumnName("i") + ", "
            + provider.escapeColumnName("id") + " as " + provider.escapeColumnName("j")
            + " FROM " + provider.escapeTableName(getTableName().getTableName()) + " WHERE ${CONDITIONS}");
    configs.getStringInput("fromJobConfig.partitionColumn").setValue("id");
    configs.getStringInput("fromJobConfig.boundaryQuery").setValue(
        "SELECT MIN(" + partitionColumn + "), MAX(" + partitionColumn + ") FROM "
            + provider.escapeTableName(getTableName().getTableName()));
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
        "1,1",
        "2,2",
        "3,3",
        "4,4",
        "5,5"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testAllowNullsWithOneExtractor() throws Exception {
    //Integration test case for SQOOP-2382
    //Server must not throw an exception when null values are allowed in the
    //partitioning column and number of extractors is set to only 1

    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getName(), hdfsConnection.getName());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    MConfigList configs = job.getFromJobConfig();
    configs.getBooleanInput("fromJobConfig.allowNullValueInPartitionColumn").setValue(true);

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
      "1,'USA','2004-10-23 00:00:00.000','San Francisco'",
      "2,'USA','2004-10-24 00:00:00.000','Sunnyvale'",
      "3,'Czech Republic','2004-10-25 00:00:00.000','Brno'",
      "4,'USA','2004-10-26 00:00:00.000','Palo Alto'",
      "5,'USA','2004-10-27 00:00:00.000','Martha\\'s Vineyard'"
    );

    // Clean up testing table
    dropTable();
  }

}
