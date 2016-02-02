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
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.asserts.HdfsAsserts;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.test.utils.ParametrizedUtils;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class NullValueTest extends SqoopTestCase {

  private static final Logger LOG = Logger.getLogger(NullValueTest.class);


  private ToFormat format;

  // The custom nullValue to use (set to null if default)
  private String nullValue;

  @DataProvider(name="nul-value-test")
  public static Object[][] data(ITestContext context) {
    String customNullValue = "^&*custom!@";

    return Iterables.toArray(
      ParametrizedUtils.crossProduct(ToFormat.values(), new String[]{SqoopIDFUtils.DEFAULT_NULL_VALUE, customNullValue}),
      Object[].class);
  }

  @Factory(dataProvider="nul-value-test")
  public NullValueTest(ToFormat format, String nullValue) {
    this.format = format;
    this.nullValue = nullValue;
  }

  @Override
  public String getTestName() {
    return methodName + "[" + format.name() + ", " + nullValue + "]";
  }

  @BeforeMethod
  public void setup() throws Exception {
    createTableCities();
  }

  @AfterMethod()
  public void dropTable() {
    super.dropTable();
  }

  private boolean usingCustomNullValue() {
    return nullValue != SqoopIDFUtils.DEFAULT_NULL_VALUE;
  }

  private String[] getCsv() {
    return new String[] {
      "1,'USA'," + nullValue + ",'San Francisco'",
      "2,'USA','2004-10-24 00:00:00.000'," + nullValue,
      "3," + nullValue + ",'2004-10-25 00:00:00.000','Brno'",
      "4,'USA','2004-10-26 00:00:00.000','Palo Alto'"
    };
  }

  @Test
  public void testFromHdfs() throws Exception {
    switch (format) {
      case TEXT_FILE:
        createFromFile("input-0001", getCsv());

        break;
      case SEQUENCE_FILE:
        SequenceFile.Writer.Option optPath =
          SequenceFile.Writer.file(new Path(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "input-0001")));
        SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(NullWritable.class);


        SequenceFile.Writer sequenceFileWriter =
          SequenceFile.createWriter(getHadoopConf(), optPath, optKey, optVal);
        for (String csv : getCsv()) {
          sequenceFileWriter.append(new Text(csv), NullWritable.get());
        }
        sequenceFileWriter.close();
        break;
      default:
        Assert.fail();
    }

    MLink hdfsLinkFrom = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLinkFrom);
    saveLink(hdfsLinkFrom);

    MLink rdbmsLinkTo = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLinkTo);
    saveLink(rdbmsLinkTo);

    MJob job = getClient().createJob(hdfsLinkFrom.getName(), rdbmsLinkTo.getName());

    fillHdfsFromConfig(job);
    fillRdbmsToConfig(job);

    if (usingCustomNullValue()) {
      job.getFromJobConfig().getBooleanInput("fromJobConfig.overrideNullValue").setValue(true);
      job.getFromJobConfig().getStringInput("fromJobConfig.nullValue").setValue(nullValue);
    }


    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    saveJob(job);

    executeJob(job);

    Assert.assertEquals(4L, provider.rowCount(getTableName()));
    assertRowInCities(1, "USA", null, "San Francisco");
    assertRowInCities(2, "USA", Timestamp.valueOf("2004-10-24 00:00:00.000"), (String) null);
    assertRowInCities(3, (String) null, Timestamp.valueOf("2004-10-25 00:00:00.000"), "Brno");
    assertRowInCities(4, "USA", Timestamp.valueOf("2004-10-26 00:00:00.000"), "Palo Alto");
  }

  @Test
  public void testToHdfs() throws Exception {
    provider.insertRow(getTableName(), 1, "USA", (java.sql.Timestamp) null, "San Francisco");
    provider.insertRow(getTableName(), 2, "USA", Timestamp.valueOf("2004-10-24 00:00:00.000"), (String) null);
    provider.insertRow(getTableName(), 3, (String) null, Timestamp.valueOf("2004-10-25 00:00:00.000"), "Brno");
    provider.insertRow(getTableName(), 4, "USA", Timestamp.valueOf("2004-10-26 00:00:00.000"), "Palo Alto");

    MLink rdbmsLinkFrom = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLinkFrom);
    saveLink(rdbmsLinkFrom);


    MLink hdfsLinkTo = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLinkTo);
    saveLink(hdfsLinkTo);

    MJob job = getClient().createJob(rdbmsLinkFrom.getName(), hdfsLinkTo.getName());

    fillRdbmsFromConfig(job, "id");
    fillHdfsToConfig(job, format);

    if (usingCustomNullValue()) {
      job.getToJobConfig().getBooleanInput("toJobConfig.overrideNullValue").setValue(true);
      job.getToJobConfig().getStringInput("toJobConfig.nullValue").setValue(nullValue);
    }

    hdfsClient.mkdirs(new Path(HdfsUtils.joinPathFragments
      (getMapreduceDirectory(), "TO")));

    job.getToJobConfig().getStringInput("toJobConfig.outputDirectory")
      .setValue(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"));


    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    saveJob(job);

    executeJob(job);

    switch (format) {
      case TEXT_FILE:
        HdfsAsserts.assertMapreduceOutput(hdfsClient,
          HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"), getCsv());
        break;
      case SEQUENCE_FILE:
        Multiset<String> setLines = HashMultiset.create(Arrays.asList(getCsv()));
        List<String> notFound = new ArrayList<>();
        Path[] files = HdfsUtils.getOutputMapreduceFiles(hdfsClient, HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"));

        for(Path file : files) {
          SequenceFile.Reader.Option optPath = SequenceFile.Reader.file(file);
          SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(getHadoopConf(), optPath);

          Text text = new Text();
          while (sequenceFileReader.next(text)) {
            if (!setLines.remove(text.toString())) {
              notFound.add(text.toString());
            }
          }
        }
        if(!setLines.isEmpty() || !notFound.isEmpty()) {
          LOG.error("Output do not match expectations.");
          LOG.error("Expected lines that weren't present in the files:");
          LOG.error("\t'" + StringUtils.join(setLines, "'\n\t'") + "'");
          LOG.error("Extra lines in files that weren't expected:");
          LOG.error("\t'" + StringUtils.join(notFound, "'\n\t'") + "'");
          Assert.fail("Output do not match expectations.");
        }
        break;
      default:
        Assert.fail();
    }
  }
}
