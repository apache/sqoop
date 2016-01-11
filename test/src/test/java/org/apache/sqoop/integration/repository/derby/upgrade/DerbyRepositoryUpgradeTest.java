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
package org.apache.sqoop.integration.repository.derby.upgrade;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.CompressionUtils;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.ITestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

/**
 * Abstract test case for testing upgrade from previous version to the "most recent one".
 *
 * In order to properly test that we can upgrade older release to a new one, we are storing
 * repository dumps created by previous releases. The test cases takes this existing repository
 * dump, un-archive it into working directly and starts the server with pointing derby to
 * this working directly. On the start up the server will perform complete upgrade to the
 * latest version, including any schema changes and connector data changes.We run several
 * tests on the repository to ensure that it's in state that we're expecting.
 *
 * Each tested version should have a child test case that is implementing the abstract
 * methods describing content of the repository (what links/jobs it have, ...).
 *
 */
@Infrastructure(dependencies = {KdcInfrastructureProvider.class})
public abstract class DerbyRepositoryUpgradeTest extends SqoopTestCase {

  protected Map<Long, String> jobIdToNameMap;
  protected Map<Long, String> linkIdToNameMap;

  private SqoopMiniCluster sqoopMiniCluster;

  /**
   * Custom Sqoop mini cluster that points derby repository to real on-disk structures.
   */
  public static class DerbySqoopMiniCluster extends JettySqoopMiniCluster {
    private String repositoryPath;

    public DerbySqoopMiniCluster(String repositoryPath, String temporaryPath, Configuration configuration) throws Exception {
      super(temporaryPath, configuration);
      this.repositoryPath = repositoryPath;
    }

    @Override
    protected Map<String, String> getRepositoryConfiguration() {
      Map<String, String> properties = new HashMap<String, String>();

      properties.put("org.apache.sqoop.repository.schema.immutable", "false");
      properties.put("org.apache.sqoop.repository.provider", "org.apache.sqoop.repository.JdbcRepositoryProvider");
      properties.put("org.apache.sqoop.repository.jdbc.handler", "org.apache.sqoop.repository.derby.DerbyRepositoryHandler");
      properties.put("org.apache.sqoop.repository.jdbc.transaction.isolation", "READ_COMMITTED");
      properties.put("org.apache.sqoop.repository.jdbc.maximum.connections", "10");
      properties.put("org.apache.sqoop.repository.jdbc.url", "jdbc:derby:" + repositoryPath);
      properties.put("org.apache.sqoop.repository.jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
      properties.put("org.apache.sqoop.repository.jdbc.user", "sa");
      properties.put("org.apache.sqoop.repository.jdbc.password", "");

      return properties;
    }
  }

  /**
   * Return resource location with the repository tarball
   */
  public abstract String getPathToRepositoryTarball();

  /**
   * Number of links that were stored in the repository
   */
  public abstract int getNumberOfLinks();

  /**
   * Number of jobs that were stored in the repository
   */
  public abstract int getNumberOfJobs();

  /**
   * Map of job id -> number of submissions that were stored in the repository
   */
  public abstract Map<Integer, Integer> getNumberOfSubmissions();

  /**
   * List of link ids that should be disabled
   */
  public abstract String[] getDisabledLinkNames();

  /**
   * List of job ids that should be disabled
   */
  public abstract String[] getDisabledJobNames();

  /**
   * List of link ids that we should delete using the id
   */
  public abstract String[] getDeleteLinkNames();

  public String getRepositoryPath() {
    return HdfsUtils.joinPathFragments(getTemporaryJettyPath(), "repo");
  }

  public String getTemporaryJettyPath() {
    return HdfsUtils.joinPathFragments(getTemporaryPath(), getClass().getCanonicalName(), getTestName());
  }

  @BeforeMethod(dependsOnMethods = { "init" })
  public void startSqoopMiniCluster(ITestContext context) throws Exception {
    // Prepare older repository structures
    InputStream tarballStream = getClass().getResourceAsStream(getPathToRepositoryTarball());
    assertNotNull(tarballStream);
    CompressionUtils.untarStreamToDirectory(tarballStream, getRepositoryPath());

    // And use them for new Derby repo instance
    sqoopMiniCluster  = new DerbySqoopMiniCluster(getRepositoryPath(), getTemporaryJettyPath() + "/sqoop-mini-cluster", getHadoopConf());

    // Start server
    sqoopMiniCluster.start();

    // Initialize Sqoop Client API
    initSqoopClient(sqoopMiniCluster.getServerUrl());

    jobIdToNameMap = new HashMap<Long, String>();
    for(MJob job : getClient().getJobs()) {
      jobIdToNameMap.put(job.getPersistenceId(), job.getName());
    }

    linkIdToNameMap = new HashMap<Long, String>();
    for(MLink link : getClient().getLinks()) {
      linkIdToNameMap.put(link.getPersistenceId(), link.getName());
    }
  }

  @AfterMethod
  public void stopSqoopMiniCluster() throws Exception {
    sqoopMiniCluster.stop();
  }

  @Test
  public void testPostUpgrade() throws Exception {

    // Please note that the upgrade itself is done on startup and hence prior calling this test
    // method. We're just verifying that Server has started and behaves and we are expecting.

    // We could further enhance the checks here, couple of ideas for the future:
    // * Add a check that will verify that the upgrade indeed happened (it's implied at the moment)
    // * Run selected jobs to ensure that they in state where they can run?

    // Verify that we have expected number of objects
    assertEquals(getNumberOfLinks(), getClient().getLinks().size());
    assertEquals(getNumberOfJobs(), getClient().getJobs().size());
    for(Map.Entry<Integer, Integer> entry : getNumberOfSubmissions().entrySet()) {
      // Skipping due to SQOOP-1782
      // assertEquals((int)entry.getValue(), getClient().getSubmissionsForJob(entry.getKey()).size());
    }

    // Verify that disabled status is preserved
    for(String linkName : getDisabledLinkNames()) {
      assertFalse(getClient().getLink(linkName).getEnabled());
    }
    for(String name : getDisabledJobNames()) {
      assertFalse(getClient().getJob(name).getEnabled());
    }

    // Remove all objects
    for(String name : jobIdToNameMap.values()) {
      getClient().deleteJob(name);
    }
    for(String linkName : getDeleteLinkNames()) {
      getClient().deleteLink(linkName);
    }

    // We should end up with empty repository
    assertEquals(0, getClient().getLinks().size());
    assertEquals(0, getClient().getJobs().size());
  }
}
