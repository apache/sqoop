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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.test.utils.ParametrizedUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;

/**
 *
 */
@RunWith(Parameterized.class)
public class PartitionerTest extends ConnectorTestCase {

  private static final Logger LOG = Logger.getLogger(PartitionerTest.class);

  /**
   * Columns that we will use as partition column with maximal number of
   * partitions that can be created for such column.
   */
  public static Object[] COLUMNS = new Object [][] {
    {"id",           13},
    {"code_name",    13},
    {"version",      13},
    {"release_date", 13},
    {"lts",           2},
  };

  /**
   * Number of extractors that we will use to transfer the table.
   */
  public static Object [] EXTRACTORS = new Object[] {
    3, 5, 10, 13,
  };

  @Parameterized.Parameters(name = "{0}-{1}-{2}")
  public static Iterable<Object[]> data() {
    return ParametrizedUtils.crossProduct(COLUMNS, EXTRACTORS);
  }

  private String partitionColumn;
  private int extractors;
  private int maxOutputFiles;

  public PartitionerTest(String partitionColumn, int expectedOutputFiles, int extractors) {
    this.partitionColumn = partitionColumn;
    this.maxOutputFiles = expectedOutputFiles;
    this.extractors = extractors;
  }

  @Test
  public void testSplitter() throws Exception {
    createAndLoadTableUbuntuReleases();

    // RDBMS connection
    MConnection rdbmsConnection = getClient().newConnection("generic-jdbc-connector");
    fillRdbmsConnectionForm(rdbmsConnection);
    createConnection(rdbmsConnection);

    // HDFS connection
    MConnection hdfsConnection = getClient().newConnection("hdfs-connector");
    createConnection(hdfsConnection);

    // Job creation
    MJob job = getClient().newJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Connector values
    MFormList forms = job.getConnectorPart(Direction.FROM);
    forms.getStringInput("fromTable.tableName").setValue(provider.escapeTableName(getTableName()));
    forms.getStringInput("fromTable.partitionColumn").setValue(provider.escapeColumnName(partitionColumn));
    fillOutputForm(job, OutputFormat.TEXT_FILE);
    forms = job.getFrameworkPart();
    forms.getIntegerInput("throttling.extractors").setValue(extractors);
    createJob(job);

    runJob(job);

    // Assert correct output
    assertMapreduceOutputFiles((extractors > maxOutputFiles) ? maxOutputFiles : extractors);
    assertMapreduceOutput(
      "1,'Warty Warthog',4.10,2004-10-20,false",
      "2,'Hoary Hedgehog',5.04,2005-04-08,false",
      "3,'Breezy Badger',5.10,2005-10-13,false",
      "4,'Dapper Drake',6.06,2006-06-01,true",
      "5,'Edgy Eft',6.10,2006-10-26,false",
      "6,'Feisty Fawn',7.04,2007-04-19,false",
      "7,'Gutsy Gibbon',7.10,2007-10-18,false",
      "8,'Hardy Heron',8.04,2008-04-24,true",
      "9,'Intrepid Ibex',8.10,2008-10-18,false",
      "10,'Jaunty Jackalope',9.04,2009-04-23,false",
      "11,'Karmic Koala',9.10,2009-10-29,false",
      "12,'Lucid Lynx',10.04,2010-04-29,true",
      "13,'Maverick Meerkat',10.10,2010-10-10,false",
      "14,'Natty Narwhal',11.04,2011-04-28,false",
      "15,'Oneiric Ocelot',11.10,2011-10-10,false",
      "16,'Precise Pangolin',12.04,2012-04-26,true",
      "17,'Quantal Quetzal',12.10,2012-10-18,false",
      "18,'Raring Ringtail',13.04,2013-04-25,false",
      "19,'Saucy Salamander',13.10,2013-10-17,false"
    );

    // Clean up testing table
    dropTable();
  }
}
