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

import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.annotations.Test;

@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class InformalObjectNameTest extends SqoopTestCase {
  private static final String LINK_NAME_CONTAINS_WHITESPACE = "link name";
  private static final String LINK_NAME_CONTAINS_SLASH = "link/name";

  private static final String JOB_NAME_CONTAINS_WHITESPACE = "job name";
  private static final String JOB_NAME_CONTAINS_SLASH= "job/name";

  public InformalObjectNameTest() {
  }

  @Test
  public void testInformalLinkName() throws Exception {
    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    rdbmsLink.setName(LINK_NAME_CONTAINS_WHITESPACE);
    saveLink(rdbmsLink);
    assertEquals(rdbmsLink, getClient().getLink(LINK_NAME_CONTAINS_WHITESPACE));

    rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    rdbmsLink.setName(LINK_NAME_CONTAINS_SLASH);
    saveLink(rdbmsLink);
    assertEquals(rdbmsLink, getClient().getLink(LINK_NAME_CONTAINS_SLASH));
  }

  @Test
  public void testInformalJobName() throws Exception {
    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLinkConfig(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getPersistenceId(), hdfsLink.getPersistenceId());

    // rdms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    job.setName(JOB_NAME_CONTAINS_WHITESPACE);
    saveJob(job);
    assertEquals(job, getClient().getJob(JOB_NAME_CONTAINS_WHITESPACE));

    job = getClient().createJob(rdbmsLink.getPersistenceId(), hdfsLink.getPersistenceId());

    // rdms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    job.setName(JOB_NAME_CONTAINS_SLASH);
    saveJob(job);

    assertEquals(job, getClient().getJob(JOB_NAME_CONTAINS_SLASH));
  }
}
