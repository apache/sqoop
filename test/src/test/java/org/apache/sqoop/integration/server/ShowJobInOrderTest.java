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

import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Ensure that jobs will be shown in order
 */
@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class})
public class ShowJobInOrderTest extends SqoopTestCase {

  public ShowJobInOrderTest() {
  }

  @Test
  public void testShowJobInOrder() throws Exception {
    createAndLoadTableCities();

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

    saveJob(job);

    // Job creation
    job = getClient().createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());

    // rdms "To" config
    fillRdbmsToConfig(job);

    // hdfs "From" config
    fillHdfsFromConfig(job);

    saveJob(job);

    // Job creation
    job = getClient().createJob(rdbmsLink.getPersistenceId(), hdfsLink.getPersistenceId());

    // rdms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    // Job creation
    job = getClient().createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());


    // hdfs "From" config
    fillHdfsFromConfig(job);

    // rdms "To" config
    fillRdbmsToConfig(job);

    saveJob(job);

    List<MJob> jobs = getClient().getJobs();

    assertEquals(1, jobs.get(0).getPersistenceId());
    assertEquals(2, jobs.get(1).getPersistenceId());
    assertEquals(3, jobs.get(2).getPersistenceId());
    assertEquals(4, jobs.get(3).getPersistenceId());
  }
}
