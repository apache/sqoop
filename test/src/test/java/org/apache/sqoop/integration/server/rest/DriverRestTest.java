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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;

import java.util.Iterator;

public class DriverRestTest extends RestTest {

  public static TestDescription[] PROVIDER_DATA = new TestDescription[]{
    new TestDescription("Get driver", "v1/driver", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
        assertContains("job-config");
        assertContains("all-config-resources");
      }}),
    new TestDescription("Invalid post request", "v1/driver", "POST", "Random data", new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(405);
        assertServerException("Unsupported HTTP method", "SERVER_0002");
      }}),
  };

  @DataProvider(name="driver-rest-test")
  public static Iterator<Object[]> data() {
    return ParametrizedUtils.toArrayOfArrays(PROVIDER_DATA).iterator();
  }

  @Factory(dataProvider = "driver-rest-test")
  public DriverRestTest(TestDescription desc) {
    super(desc);
  }
}
