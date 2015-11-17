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
package org.apache.sqoop.integration.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Iterables;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.utils.ParametrizedUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class InformalObjectNameTest extends SqoopTestCase {

  private String target;
  private String specialChar;

  private static final String TARGET_JOB = "Job";
  private static final String TARGET_LINK = "Link";

  /**
   * The object used for test, job and link.
   */
  public static Object[] TARGETS = new Object [] {
          TARGET_JOB,
          TARGET_LINK,
  };

  /**
   * The special char used for test.
   */
  public static Object [] SPECIAL_CHAR = new Object[] {
          " ", "\t", "/", ".", "?", "&", "*", "[", "]", "(", ")", "`", "~", "!", "@",
          "#", "$", "%", "^", "-", "_", "=", "+", ";", ":", "\"", "<", ">", ",",
  };

  @Factory(dataProvider="special-name-integration-test")
  public InformalObjectNameTest(String target, String specialChar) {
    this.target = target;
    this.specialChar = specialChar;
  }

  @DataProvider(name="special-name-integration-test", parallel=true)
  public static Object[][] data() {
    return Iterables.toArray(ParametrizedUtils.crossProduct(TARGETS, SPECIAL_CHAR), Object[].class);
  }

  @Test
  public void testInformalName() throws Exception {
    if (TARGET_LINK.equals(target)) {
      verifyActionsForLink("link" + specialChar + "name");
    } else if (TARGET_JOB.equals(target)) {
      verifyActionsForJob("job" + specialChar + "name");
    }
  }

  private void verifyActionsForLink(String linkName) {
    // create link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    rdbmsLink.setName(linkName);
    saveLink(rdbmsLink);
    // read link
    assertEquals(rdbmsLink, getClient().getLink(linkName));

    // update link
    getClient().updateLink(rdbmsLink);

    // enable link
    getClient().enableLink(linkName, true);

    // delete link
    getClient().deleteLink(linkName);
    try {
      getClient().getLink(linkName);
      fail("The link doesn't exist, exception should be thrown.");
    } catch (Exception e) {
      // ignore the exception
    }
  }

  private void verifyActionsForJob(String jobName) throws Exception {
    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLinkConfig(hdfsLink);
    saveLink(hdfsLink);


    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), hdfsLink.getName());

    // rdms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    job.setName(jobName);
    saveJob(job);

    // read job
    assertEquals(job, getClient().getJob(jobName));

    // update job
    getClient().updateJob(job);

    // enable job
    getClient().enableJob(jobName, true);

    // delete job
    getClient().deleteJob(jobName);
    try {
      getClient().getJob(jobName);
      fail("The job doesn't exist, exception should be thrown.");
    } catch (Exception e) {
      // ignore the exception
    }
  }
}
