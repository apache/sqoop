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
package org.apache.sqoop.integration.server.rest;

import org.apache.log4j.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Infrastructure(dependencies = {HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class})
public abstract class RestTest extends SqoopTestCase {

  private static final Logger LOG = Logger.getLogger(RestTest.class);

  // Validate returned response from server
  public static abstract class Validator {

    // Persisted connection object that we're testing against
    HttpURLConnection connection;

    // Persisted data from connection
    String error;
    String input;

    public void setConnection(HttpURLConnection connection) throws Exception {
      this.connection = connection;

      this.input = "";
      try {
        this.input = (connection.getInputStream() != null) ? IOUtils.toString(connection.getInputStream()) : "";
      } catch(Exception e) {
        // We're ignoring exception here because that means that request wasn't successful and response is in "error" stream instead
      }
      this.error = connection.getErrorStream() != null ? IOUtils.toString(connection.getErrorStream()) : "";
    }

    // Each test should implement whatever is needed here
    abstract void validate() throws Exception;

    // Verify HTTP response code
    public void assertResponseCode(int expected) throws Exception {
      assertEquals(connection.getResponseCode(), expected);
    }

    // Assert given exception from server
    public void assertServerException(String errorClass, String errorCode) throws Exception {
      // On exception, the error trace can't be null
      assertNotNull(error);

      // We're not parsing entire JSON, but rather just looking for sub-strings that are of particular interest
      assertTrue(error.contains("error-code-class\":\"" + errorClass));
      assertTrue(error.contains("error-code\":\"" + errorCode));
    }

    public void assertContains(String subString) throws Exception {
      assertTrue(responseString().contains(subString), "Server response doesn't contain: " + subString);
    }

    private String responseString() {
      if(input.isEmpty()) {
        return error;
      } else {
        return input;
      }
    }
  }

  // Small internal class describing our test case
  public static class TestDescription {
    public String name;       // Name of the test
    public String rest;       // Rest endpoint where we'll send request
    public String method;     // Method that we should use
    public String data;       // Data that we'll be sending as part of POST (NULL for nothing)
    public Validator validator; // Routine to validate the response from the server

    public TestDescription(String name, String rest, String method, String data, Validator validator) {
      this.name = name;
      this.rest = rest;
      this.method = method;
      this.data = data;
      this.validator = validator;
    }
  }

  /**
   * Various objects that can be pre-created by child test cases
   */
  public void createFirstLink() {
    // Link: first-link
    MLink genericJDBCLink = getClient().createLink("generic-jdbc-connector");
    genericJDBCLink.setName("first-link");
    MConfigList configs = genericJDBCLink.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.jdbcDriver").setValue("org.apache.derby.jdbc.ClientDriver");
    configs.getStringInput("linkConfig.connectionString").setValue("jdbc:derby:memory:invalid-rest-calls-test;create=true");
    configs.getStringInput("linkConfig.username").setValue("sqoop");
    configs.getStringInput("linkConfig.password").setValue("is-awesome");
    getClient().saveLink(genericJDBCLink);
  }

  @AfterMethod
  public void dropTestData() {
    for(MJob job : getClient().getJobs()) {
      getClient().deleteJob(job.getName());
    }
    for(MLink link : getClient().getLinks()) {
      getClient().deleteLink(link.getName());
    }
  }

  private TestDescription desc;

  public RestTest(TestDescription desc) {
    this.desc = desc;
  }

  @Override
  public String getTestName() {
    return getClass().getName() + " " + desc.rest + "[" + desc.method + "]: " + desc.name;
  }

  @Test
  public void test() throws Exception {
    LOG.info("Start: " + getTestName());

    URL url = new URL(getSqoopServerUrl() +  desc.rest);
    HttpURLConnection connection = new DelegationTokenAuthenticatedURL().openConnection(url, new DelegationTokenAuthenticatedURL.Token());
    connection.setRequestMethod(desc.method);

    if(desc.data != null) {
      connection.setDoOutput(true);

      byte[] byteData = desc.data.getBytes(Charset.forName("UTF-8"));
      connection.setRequestProperty("Content-Length", Integer.toString(byteData.length));
      DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
      wr.write(byteData, 0, byteData.length);
      wr.flush();
      wr.close();
    }

    desc.validator.setConnection(connection);
    LOG.info("error = " + desc.validator.error);
    LOG.info("input = " + desc.validator.input);
    desc.validator.validate();

    LOG.info("End: " + getTestName());
  }

}
