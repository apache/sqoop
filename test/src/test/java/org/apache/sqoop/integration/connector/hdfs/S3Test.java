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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.connector.hdfs.configuration.IncrementalType;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.asserts.HdfsAsserts;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Run with something like:
 *
 * mvn clean test -pl test -Dtest=S3Test
 *   -Dorg.apache.sqoop.integration.connector.hdfs.s3.bucket=test-bucket
 *   -Dorg.apache.sqoop.integration.connector.hdfs.s3.access=AKI...
 *   -Dorg.apache.sqoop.integration.connector.hdfs.s3.secret=93JKx...
 */
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class S3Test extends SqoopTestCase {

  public static final String PROPERTY_BUCKET = "org.apache.sqoop.integration.connector.hdfs.s3.bucket";
  public static final String PROPERTY_ACCESS = "org.apache.sqoop.integration.connector.hdfs.s3.access";
  public static final String PROPERTY_SECRET = "org.apache.sqoop.integration.connector.hdfs.s3.secret";

  public static final String BUCKET = System.getProperty(PROPERTY_BUCKET);
  public static final String ACCESS = System.getProperty(PROPERTY_ACCESS);
  public static final String SECRET = System.getProperty(PROPERTY_SECRET);

  public static final String BUCKET_URL = "s3a://" + BUCKET;

  // S3 client (HDFS interface) to be used in the tests
  private FileSystem s3Client;

  public void setUpS3Client() throws Exception {
    assumeNotNull(BUCKET, PROPERTY_BUCKET);
    assumeNotNull(ACCESS, PROPERTY_ACCESS);
    assumeNotNull(SECRET, PROPERTY_SECRET);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", BUCKET_URL);
    hadoopConf.set("fs.s3a.access.key", ACCESS);
    hadoopConf.set("fs.s3a.secret.key", SECRET);
    s3Client = FileSystem.get(hadoopConf);
  }

  @Test
  public void testImportExport() throws Exception {
    setUpS3Client();
    s3Client.delete(new Path(getMapreduceDirectory()), true);
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    hdfsLink.getConnectorLinkConfig().getStringInput("linkConfig.uri").setValue(BUCKET_URL);
    Map<String, String> configOverrides = new HashMap<>();
    configOverrides.put("fs.s3a.access.key", ACCESS);
    configOverrides.put("fs.s3a.secret.key", SECRET);
    hdfsLink.getConnectorLinkConfig().getMapInput("linkConfig.configOverrides").setValue(configOverrides);
    saveLink(hdfsLink);

    // DB -> S3
    MJob db2aws = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());
    fillRdbmsFromConfig(db2aws, "id");
    fillHdfsToConfig(db2aws, ToFormat.TEXT_FILE);

    saveJob(db2aws);
    executeJob(db2aws);

    // Verifying locally imported data
    HdfsAsserts.assertMapreduceOutput(s3Client, getMapreduceDirectory(),
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );

    // This re-creates the table completely
    createTableCities();
    assertEquals(provider.rowCount(getTableName()), 0);

    // S3 -> DB
    MJob aws2db = getClient().createJob(hdfsLink.getName(), rdbmsLink.getName());
    fillHdfsFromConfig(aws2db);
    fillRdbmsToConfig(aws2db);

    saveJob(aws2db);
    executeJob(aws2db);

    // Final verification
    assertEquals(4L, provider.rowCount(getTableName()));
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");
    assertRowInCities(2, "USA", "2004-10-24", "Sunnyvale");
    assertRowInCities(3, "Czech Republic", "2004-10-25", "Brno");
    assertRowInCities(4, "USA", "2004-10-26", "Palo Alto");
  }

  @Test
  public void testIncrementalRead() throws Exception {
    setUpS3Client();
    s3Client.delete(new Path(getMapreduceDirectory()), true);

    // S3 link
    MLink s3Link = getClient().createLink("hdfs-connector");
    s3Link.getConnectorLinkConfig().getStringInput("linkConfig.uri").setValue(BUCKET_URL);
    Map<String, String> configOverrides = new HashMap<>();
    configOverrides.put("fs.s3a.access.key", ACCESS);
    configOverrides.put("fs.s3a.secret.key", SECRET);
    s3Link.getConnectorLinkConfig().getMapInput("linkConfig.configOverrides").setValue(configOverrides);
    saveLink(s3Link);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // S3 -> HDFS
    MJob aws2hdfs = getClient().createJob(s3Link.getName(), hdfsLink.getName());
    fillHdfsFromConfig(aws2hdfs);
    aws2hdfs.getFromJobConfig().getEnumInput("incremental.incrementalType").setValue(IncrementalType.NEW_FILES);

    fillHdfsToConfig(aws2hdfs, ToFormat.TEXT_FILE);
    aws2hdfs.getToJobConfig().getBooleanInput("toJobConfig.appendMode").setValue(true);
    saveJob(aws2hdfs);

    // First import (first file)
    createFromFile(s3Client, "input-0001",
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'"
    );
    executeJob(aws2hdfs);

    HdfsAsserts.assertMapreduceOutput(hdfsClient, getMapreduceDirectory(),
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'"
    );

    // Second import (second file)
    createFromFile(s3Client, "input-0002",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );
    executeJob(aws2hdfs);

    HdfsAsserts.assertMapreduceOutput(hdfsClient, getMapreduceDirectory(),
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );
  }

  void assumeNotNull(String value, String key) {
    if(value == null) {
      throw new SkipException("Missing value for " + key);
    }
  }

}
