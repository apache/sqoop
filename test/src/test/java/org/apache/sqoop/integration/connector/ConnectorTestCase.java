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
package org.apache.sqoop.integration.connector;

import org.apache.log4j.Logger;
import org.apache.sqoop.integration.TomcatTestCase;
import org.apache.sqoop.test.db.DatabaseProvider;
import org.apache.sqoop.test.db.DatabaseProviderFactory;
import org.junit.After;
import org.junit.Before;

/**
 * Base test case for connector testing.
 *
 * It will create and initialize database provider prior every test execution.
 */
abstract public class ConnectorTestCase extends TomcatTestCase {

  private static final Logger LOG = Logger.getLogger(ConnectorTestCase.class);

  protected DatabaseProvider provider;

  @Before
  public void startProvider() throws Exception {
    provider = DatabaseProviderFactory.getProvider(System.getProperties());
    LOG.info("Starting database provider: " + provider.getClass().getName());
    provider.start();
  }

  @After
  public void stopProvider() {
    provider.stop();
  }

  public String getTableName() {
    return getClass().getSimpleName();
  }

  protected void createTable(String primaryKey, String ...columns) {
    provider.createTable(getTableName(), primaryKey, columns);
  }

  protected void dropTable() {
    provider.dropTable(getTableName());
  }

  protected void insertRow(Object ...values) {
    provider.insertRow(getTableName(), values);
  }
}
