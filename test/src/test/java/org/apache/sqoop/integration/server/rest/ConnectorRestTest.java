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

public class ConnectorRestTest extends RestTest {

  public static TestDescription[] PROVIDER_DATA = new TestDescription[]{
    new TestDescription("Get all connectors", "v1/connector/all", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
      }}),
    new TestDescription("Get connector by ID", "v1/connector/1", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
      }}),
    new TestDescription("Get connector by name", "v1/connector/generic-jdbc-connector", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(200);
      }}),
    new TestDescription("Get connector by non-existing ID", "v1/connector/666", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("org.apache.sqoop.server.common.ServerError", "SERVER_0006");
      }}),
    new TestDescription("Get connector by non-existing name", "v1/connector/jarcecs-cool-connector", "GET", null, new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("org.apache.sqoop.server.common.ServerError", "SERVER_0005");
      }}),
    new TestDescription("Invalid post request", "v1/connector", "POST", "Random data", new Validator() {
      @Override
      void validate() throws Exception {
        assertResponseCode(500);
        assertServerException("org.apache.sqoop.server.common.ServerError", "SERVER_0002");
      }}),
  };

  @DataProvider(name="connector-rest-test", parallel=false)
  public static Iterator<Object[]> data() {
    return ParametrizedUtils.toArrayOfArrays(PROVIDER_DATA).iterator();
  }

  @Factory(dataProvider = "connector-rest-test")
  public ConnectorRestTest(TestDescription desc) {
    super(desc);
  }
}
