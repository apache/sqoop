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

import org.apache.hadoop.fs.Path;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.asserts.HdfsAsserts;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.Test;

/**
 * Test schemaless to schemaless transfer by using two hdfs connectors
 */
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class})
public class FromHDFSToHDFSTest extends SqoopTestCase {

  @Test
  public void test() throws Exception {
    String[] sampleData = new String[]{
      "1,'USA','2004-10-23 00:00:00.000','San Francisco'",
      "2,'USA','2004-10-24 00:00:00.000','Sunnyvale'",
      "3,'Czech Republic','2004-10-25 00:00:00.000','Brno'",
      "4,'USA','2004-10-26 00:00:00.000','Palo Alto'",
      "5,'USA','2004-10-27 00:00:00.000','Martha\\'s Vineyard'"
    };

    createFromFile("input-0001", sampleData);

    MLink hdfsLinkFrom = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLinkFrom);
    saveLink(hdfsLinkFrom);

    MLink hdfsLinkTo = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLinkTo);
    saveLink(hdfsLinkTo);

    MJob job = getClient().createJob(hdfsLinkFrom.getName(), hdfsLinkTo.getName());

    fillHdfsFromConfig(job);

    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    hdfsClient.mkdirs(new Path(HdfsUtils.joinPathFragments
      (getMapreduceDirectory(), "TO")));

    job.getToJobConfig().getStringInput("toJobConfig.outputDirectory")
      .setValue(HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"));


    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    saveJob(job);

    executeJob(job);

    HdfsAsserts.assertMapreduceOutput(hdfsClient, HdfsUtils.joinPathFragments(getMapreduceDirectory(), "TO"), sampleData);
  }
}
