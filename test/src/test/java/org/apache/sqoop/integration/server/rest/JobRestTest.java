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

import org.apache.sqoop.test.utils.ParametrizedUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;

import java.util.Iterator;

public class JobRestTest extends RestTest {

  @BeforeMethod
  public void setUp() {
    createFirstJob();
  }

  private static Validator firstJob = new Validator() {
    @Override
    void validate() throws Exception {
      assertResponseCode(200);
      assertContains("first-job");
    }
  };

  public static TestDescription[] PROVIDER_DATA = new TestDescription[]{
    // Get
    new TestDescription("Get all jobs", "v1/job/all", "GET", null, firstJob),
    new TestDescription("Get job by name", "v1/job/first-job", "GET", null, firstJob),
    new TestDescription("Get all jobs for connector", "v1/job/all?cname=hdfs-connector", "GET", null, firstJob),
    new TestDescription("Get non existing job", "v1/job/i-dont-exists", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("Entity requested doesn't exist", "SERVER_0006");
        assertContains("Job: i-dont-exists doesn't exist");
      }}),
    new TestDescription("Get jobs for non existing connector", "v1/job/all?cname=i-dont-exists", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("Entity requested doesn't exist", "SERVER_0006");
        assertContains("Invalid connector: i-dont-exists");
      }}),
  };

  @DataProvider(name="job-rest-test")
  public static Iterator<Object[]> data() {
    return ParametrizedUtils.toArrayOfArrays(PROVIDER_DATA).iterator();
  }

  @Factory(dataProvider = "job-rest-test")
  public JobRestTest(TestDescription desc) {
    super(desc);
  }
}
