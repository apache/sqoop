/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.sqoop.repository;

import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.sqoop.repository.JdbcTransactionIsolation.READ_COMMITTED;
import static org.apache.sqoop.repository.RepoConfigurationConstants.*;
import static org.testng.Assert.assertEquals;

public class TestJdbcRepositoryContext {

  private JdbcRepositoryContext jdbcRepositoryContext;

  private Map<String, String> propertyMap;

  @BeforeMethod
  public void setUp() {
    propertyMap = buildDefaultPropertyMap();
  }

  @Test
  public void testValidJdbcHandlerClassName() {
    final String handlerClass = "handlerClass";

    propertyMap.put(SYSCFG_REPO_JDBC_HANDLER, handlerClass);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(handlerClass, jdbcRepositoryContext.getHandlerClassName());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testInvalidJdbcHandlerClassName() {
    propertyMap.put(SYSCFG_REPO_JDBC_HANDLER, null);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  @Test
  public void testConnectionUrl() {
    final String url = "jdbcUrl";
    propertyMap.put(SYSCFG_REPO_JDBC_URL, url);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(url, jdbcRepositoryContext.getConnectionUrl());
  }

  @Test
  public void testDriverClass() {
    final String driverClass = "jdbcDriver";
    propertyMap.put(SYSCFG_REPO_JDBC_DRIVER, driverClass);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(driverClass, jdbcRepositoryContext.getDriverClass());
  }

  @Test
  public void testPasswordGenerator() {
    final String passwordGenerator = "echo secret";
    propertyMap.put(SYSCFG_REPO_JDBC_PASSWORD_GENERATOR, passwordGenerator);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals("secret", jdbcRepositoryContext.getConnectionProperties().get("password"));

  }

  @Test
  public void testPasswordString() {
    final String password = "secret";
    propertyMap.put(SYSCFG_REPO_JDBC_PASSWORD, password);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(password, jdbcRepositoryContext.getConnectionProperties().get("password"));

  }

  @Test
  public void testNestedPasswordString() {
    final String nestedPassword = "nestedPassword";
    propertyMap.put(PREFIX_SYSCFG_REPO_JDBC_PROPERTIES + "password", nestedPassword);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(nestedPassword, jdbcRepositoryContext.getConnectionProperties().get("password"));
  }

  @Test
  public void testExplicitPasswordOverridesNestedPassword() {
    final String nestedPassword = "nestedPassword";
    final String password = "secret";
    propertyMap.put(SYSCFG_REPO_JDBC_PASSWORD, password);
    propertyMap.put(PREFIX_SYSCFG_REPO_JDBC_PROPERTIES + "password", nestedPassword);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(password, jdbcRepositoryContext.getConnectionProperties().get("password"));
  }

  @Test
  public void testNestedUser() {
    final String nestedUser = "nestedUser";
    propertyMap.put(PREFIX_SYSCFG_REPO_JDBC_PROPERTIES + "user", nestedUser);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(nestedUser, jdbcRepositoryContext.getConnectionProperties().get("user"));
  }

  @Test
  public void testExplicitUserOverridesNestedUser() {
    final String nestedUser = "nestedUser";
    final String user = "user";
    propertyMap.put(SYSCFG_REPO_JDBC_USER, user);
    propertyMap.put(PREFIX_SYSCFG_REPO_JDBC_PROPERTIES + "user", nestedUser);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(user, jdbcRepositoryContext.getConnectionProperties().get("user"));
  }

  @Test
  public void testValidMaxConnection() {
    final String maxConnection = "20";

    propertyMap.put(SYSCFG_REPO_JDBC_MAX_CONN, maxConnection);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));

    assertEquals(Integer.parseInt(maxConnection), jdbcRepositoryContext.getMaximumConnections());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testWithoutTxIsolation() {
    propertyMap.put(SYSCFG_REPO_JDBC_TX_ISOLATION, null);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testInvalidTxIsolation() {
    propertyMap.put(SYSCFG_REPO_JDBC_TX_ISOLATION, "INVALID_ISOLATION");
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testWithoutMaxConnection() {
    propertyMap.put(SYSCFG_REPO_JDBC_MAX_CONN, null);
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testWithNaNMaxConnection() {
    propertyMap.put(SYSCFG_REPO_JDBC_MAX_CONN, "INVALID_MAX_CONN");
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNegativeMaxConnection() {
    propertyMap.put(SYSCFG_REPO_JDBC_MAX_CONN, "-1");
    jdbcRepositoryContext = new JdbcRepositoryContext(new MapContext(propertyMap));
  }

  private Map<String, String> buildDefaultPropertyMap() {
    Map<String, String> propertyMap = new HashMap<>();
    propertyMap.put(SYSCFG_REPO_JDBC_HANDLER, "handler");
    propertyMap.put(SYSCFG_REPO_JDBC_TX_ISOLATION, READ_COMMITTED.toString());
    propertyMap.put(SYSCFG_REPO_JDBC_MAX_CONN, "10");

    return propertyMap;
  }

}