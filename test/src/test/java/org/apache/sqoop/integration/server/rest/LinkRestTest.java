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

public class LinkRestTest extends RestTest {

  @BeforeMethod
  public void setUp() {
    createFirstLink();
  }

  public static TestDescription[] PROVIDER_DATA = new TestDescription[]{
    // Get
    new TestDescription("Get all links", "v1/link/all", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
        assertContains("first-link");
      }}),
    new TestDescription("Get link by name", "v1/link/first-link", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
        assertContains("first-link");
      }}),
    new TestDescription("Get all links for connector", "v1/link/all?cname=generic-jdbc-connector", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
        assertContains("first-link");
      }}),
    new TestDescription("Get non existing link", "v1/link/i-dont-exists", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("org.apache.sqoop.server.common.ServerError", "SERVER_0006");
        assertContains("Invalid link name: i-dont-exists");
      }}),
    new TestDescription("Get links for non existing connector", "v1/link/all?cname=i-dont-exists", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("org.apache.sqoop.server.common.ServerError", "SERVER_0005");
        assertContains("Invalid connector: i-dont-exists");
      }}),
  };

  @DataProvider(name="link-rest-test", parallel=false)
  public static Iterator<Object[]> data() {
    return ParametrizedUtils.toArrayOfArrays(PROVIDER_DATA).iterator();
  }

  @Factory(dataProvider = "link-rest-test")
  public LinkRestTest(TestDescription desc) {
    super(desc);
  }
}
