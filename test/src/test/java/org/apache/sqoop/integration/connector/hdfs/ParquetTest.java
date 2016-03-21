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
package org.apache.sqoop.integration.connector.hdfs;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.connector.hdfs.hdfsWriter.HdfsParquetWriter;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class ParquetTest extends SqoopTestCase {

  @AfterMethod
  public void dropTable() {
    super.dropTable();
  }

  @Test
  public void toParquetTest() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    hdfsClient.mkdirs(new Path(HdfsUtils.joinPathFragments
      (getMapreduceDirectory(), "TO")));

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getName(), hdfsConnection.getName());




    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // Fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.PARQUET_FILE);

    job.getToJobConfig().getStringInput("toJobConfig.outputDirectory")
      .setValue(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"));

    saveJob(job);
    executeJob(job);

    String[] expectedOutput =
      {"'1','USA','2004-10-23 00:00:00.000','San Francisco'",
        "'2','USA','2004-10-24 00:00:00.000','Sunnyvale'",
        "'3','Czech Republic','2004-10-25 00:00:00.000','Brno'",
        "'4','USA','2004-10-26 00:00:00.000','Palo Alto'"};


    Multiset<String> setLines = HashMultiset.create(Arrays.asList(expectedOutput));

    List<String> notFound = new LinkedList<>();

    Path[] files = HdfsUtils.getOutputMapreduceFiles(hdfsClient, HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"));
    for (Path file : files) {
      ParquetReader<GenericRecord> avroParquetReader = AvroParquetReader.builder(file).build();
      GenericRecord record;
      while ((record = avroParquetReader.read()) != null) {
        String recordAsLine = recordToLine(record);
        if (!setLines.remove(recordAsLine)) {
          notFound.add(recordAsLine);
        }
      }
    }

    if (!setLines.isEmpty() || !notFound.isEmpty()) {
      fail("Output do not match expectations.");
    }
  }

  @Test
  public void fromParquetTest() throws Exception {
    createTableCities();

    Schema sqoopSchema = new Schema("cities");
    sqoopSchema.addColumn(new FixedPoint("id", Long.valueOf(Integer.SIZE), true));
    sqoopSchema.addColumn(new Text("country"));
    sqoopSchema.addColumn(new DateTime("some_date", true, false));
    sqoopSchema.addColumn(new Text("city"));

    HdfsParquetWriter parquetWriter = new HdfsParquetWriter();

    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, hdfsClient.getUri());

    parquetWriter.initialize(
      new Path(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "input-0001.parquet")),
      sqoopSchema, conf, null);

    parquetWriter.write("1,'USA','2004-10-23 00:00:00.000','San Francisco'");
    parquetWriter.write("2,'USA','2004-10-24 00:00:00.000','Sunnyvale'");

    parquetWriter.destroy();

    parquetWriter.initialize(
      new Path(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "input-0002.parquet")),
      sqoopSchema, conf, null);

    parquetWriter.write("3,'Czech Republic','2004-10-25 00:00:00.000','Brno'");
    parquetWriter.write("4,'USA','2004-10-26 00:00:00.000','Palo Alto'");

    parquetWriter.destroy();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(hdfsLink.getName(), rdbmsLink.getName());
    fillHdfsFromConfig(job);
    fillRdbmsToConfig(job);
    saveJob(job);

    executeJob(job);
    assertEquals(provider.rowCount(getTableName()), 4);
    assertRowInCities(1, "USA", Timestamp.valueOf("2004-10-23 00:00:00.000"), "San Francisco");
    assertRowInCities(2, "USA", Timestamp.valueOf("2004-10-24 00:00:00.000"), "Sunnyvale");
    assertRowInCities(3, "Czech Republic", Timestamp.valueOf("2004-10-25 00:00:00.000"), "Brno");
    assertRowInCities(4, "USA", Timestamp.valueOf("2004-10-26 00:00:00.000"), "Palo Alto");
  }

  public String recordToLine(GenericRecord genericRecord) {
    String line = "";
    line += "\'" + String.valueOf(genericRecord.get(0)) + "\',";
    line += "\'" + String.valueOf(genericRecord.get(1)) + "\',";
    line += "\'" + new Timestamp((Long)genericRecord.get(2)) + "00\',";
    line += "\'" + String.valueOf(genericRecord.get(3)) + "\'";
    return line;
  }

}
