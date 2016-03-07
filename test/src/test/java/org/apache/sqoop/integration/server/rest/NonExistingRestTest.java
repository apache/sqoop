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

public class NonExistingRestTest extends RestTest {

  // Various HTTP codes
  private static Validator notFoundValidator = new Validator() {
    @Override
    void validate() throws Exception {
      assertResponseCode(404);
    }
  };
  private static Validator notAllowedValidator = new Validator() {
    @Override
    void validate() throws Exception {
      assertResponseCode(405);
    }
  };

  public static TestDescription[] PROVIDER_DATA = new TestDescription[]{
    // Various non-existing end-points and HTTP methods
    new TestDescription("Get to nowhere", "my/cool/endpoint", "GET", null, notFoundValidator),
    new TestDescription("Get to nowhere in v1 space", "v1/my/cool/endpoint", "GET", null, notFoundValidator),
    new TestDescription("Delete to nowhere", "my/cool/endpoint", "DELETE", null, notAllowedValidator),
    new TestDescription("Delete to nowhere in v1 space", "v1/my/cool/endpoint", "DELETE", null, notAllowedValidator),
    new TestDescription("Post to nowhere", "my/cool/endpoint", "POST", "{}", notAllowedValidator),
    new TestDescription("Post to nowhere in v1 space", "v1/my/cool/endpoint", "POST", "{}", notAllowedValidator),
    new TestDescription("Put to nowhere", "my/cool/endpoint", "PUT", "{}", notAllowedValidator),
    new TestDescription("Put to nowhere in v1 space", "v1/my/cool/endpoint", "PUT", "{}", notAllowedValidator),

    // Content changing requests for read-only resources
    new TestDescription("Put to version", "version", "PUT", "{}", notAllowedValidator),
    new TestDescription("POST to connector", "v1/connector", "POST", "{}", notAllowedValidator),
    new TestDescription("DELETE to submission", "v1/submission", "DELETE", null, notAllowedValidator),
  };

  @DataProvider(name="non-existing-rest-test")
  public static Iterator<Object[]> data() {
    return ParametrizedUtils.toArrayOfArrays(PROVIDER_DATA).iterator();
  }

  @Factory(dataProvider = "non-existing-rest-test")
  public NonExistingRestTest(TestDescription desc) {
    super(desc);
  }
}
