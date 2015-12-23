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
package org.apache.sqoop.integration.tools;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.JobsBean;
import org.apache.sqoop.json.LinksBean;
import org.apache.sqoop.json.SubmissionsBean;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.apache.sqoop.tools.tool.JSONConstants;
import org.apache.sqoop.tools.tool.RepositoryDumpTool;
import org.apache.sqoop.tools.tool.RepositoryLoadTool;
import org.apache.sqoop.utils.UrlSafeUtils;
import org.json.simple.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RepositoryDumpLoadToolTest extends ConnectorTestCase {

  private static final Logger LOG = Logger.getLogger(RepositoryDumpLoadToolTest.class);
  private String jsonFilePath;

  // do the load test and insert data to repo first, then do the dump test.
  @Test(dependsOnMethods = { "testLoad" })
  public void testDump() throws Exception {
    // dump the repository
    RepositoryDumpTool rdt = new RepositoryDumpTool();
    rdt.setInTest(true);
    String fileName = HdfsUtils.joinPathFragments(getTemporaryPath(), "repoDumpTest.json");
    rdt.runToolWithConfiguration(new String[]{"-o", fileName});

    // load the output json file and do the verification
    try (InputStream input = new FileInputStream(fileName)) {
      String jsonTxt = IOUtils.toString(input, Charsets.UTF_8);
      JSONObject json = JSONUtils.parse(jsonTxt);
      JSONObject metadata = (JSONObject) json.get(JSONConstants.METADATA);
      assertEquals((String) metadata.get(JSONConstants.VERSION), VersionInfo.getBuildVersion());

      // verify the links
      JSONObject jsonLinks = (JSONObject) json.get(JSONConstants.LINKS);
      LinksBean linksBean = new LinksBean();
      linksBean.restore(jsonLinks);
      verifyLinks(linksBean.getLinks());

      // verify the job
      JSONObject jsonJobs = (JSONObject) json.get(JSONConstants.JOBS);
      JobsBean jobsBean = new JobsBean();
      jobsBean.restore(jsonJobs);
      verifyJobs(jobsBean.getJobs());

      // verify the submission
      JSONObject jsonSubmissions = (JSONObject) json.get(JSONConstants.SUBMISSIONS);
      SubmissionsBean submissionsBean = new SubmissionsBean();
      submissionsBean.restore(jsonSubmissions);
      verifySubmissions(submissionsBean.getSubmissions());
    }
  }

  @Test
  public void testLoad() throws Exception {
    RepositoryLoadTool rlt = new RepositoryLoadTool();
    rlt.setInTest(true);
    rlt.runToolWithConfiguration(new String[]{"-i", jsonFilePath});
    verifyLinks(getClient().getLinks());
    verifyJobs(getClient().getJobs());
    verifySubmissions(getClient().getSubmissions());
  }

  private void verifyLinks(List<MLink> links) {
    for (MLink link : links) {
      String linkName = link.getName();
      assertTrue("hdfsLink1".equals(linkName) || "hdfsLink2".equals(linkName));
      if ("hdfsLink1".equals(linkName)) {
        assertEquals(link.getConnectorName(), "hdfs-connector");
      } else {
        assertEquals(link.getConnectorName(), "hdfs-connector");
      }
    }
  }

  private void verifyJobs(List<MJob> jobs) {
    assertEquals(jobs.size(), 1);
    MJob job = jobs.get(0);
    assertEquals(job.getFromConnectorName(), "hdfs-connector");
    assertEquals(job.getToConnectorName(), "hdfs-connector");
    assertEquals(job.getFromLinkName(), "hdfsLink1");
    assertEquals(job.getToLinkName(), "hdfsLink2");
    assertEquals(job.getName(), "jobName");
  }

  private void verifySubmissions(List<MSubmission> submissions) {
    assertEquals(submissions.size(), 1);
    MSubmission submission = submissions.get(0);
    assertEquals(submission.getJobName(), "jobName");
    assertEquals(submission.getStatus(), SubmissionStatus.SUCCEEDED);
  }

  // generate the json file without the license
  @BeforeMethod
  public void prepareJsonFile() throws Exception {
    String testFilePath = getClass().getResource("/repoLoadToolTest.json").getPath();
    jsonFilePath = HdfsUtils.joinPathFragments(getTemporaryPath(), "repoLoadTest.json");
    try (BufferedReader reader = new BufferedReader(new FileReader(testFilePath));
        FileWriter writer = new FileWriter(jsonFilePath)) {
      String line;
      while ((line = reader.readLine()) != null) {
        // ignore the license line
        if (!line.startsWith("#")) {
          // for hdfs connector, DirectoryExistsValidator is responsible for validation
          // replace the link config dir by the local path.
          if  (line.indexOf("linkConfReplacement") > 0) {
            line = line.replaceAll("linkConfReplacement", UrlSafeUtils.urlEncode(getSqoopMiniClusterTemporaryPath() + "/config/"));
          }
          writer.write(line);
        }
      }
      writer.flush();
    }
  }
}
