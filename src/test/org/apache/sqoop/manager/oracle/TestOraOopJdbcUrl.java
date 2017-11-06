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

package org.apache.sqoop.manager.oracle;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.sqoop.manager.oracle.OraOopUtilities.
           JdbcOracleThinConnectionParsingError;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for OraOopJdbcUrl.
 */
public class TestOraOopJdbcUrl extends OraOopTestCase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testParseJdbcOracleThinConnectionString() {

    OraOopUtilities.JdbcOracleThinConnection actual;

    // Null JDBC URL...
    try {
      actual = new OraOopJdbcUrl(null).parseJdbcOracleThinConnectionString();
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail("An IllegalArgumentException should be been thrown.");
    }

    // Empty JDBC URL...
    try {
      actual = new OraOopJdbcUrl("").parseJdbcOracleThinConnectionString();
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail("An IllegalArgumentException should be been thrown.");
    }

    // Incorrect number of fragments in the URL...
    try {
      actual =
          new OraOopJdbcUrl("jdbc:oracle:oci8:@dbname.domain")
              .parseJdbcOracleThinConnectionString();
      fail(
          "A JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      // This is what we want to happen.
      assertTrue(
          "An exception should be thown that tells us there's an incorrect "
          + "number of fragments in the JDBC URL.",
          ex.getMessage()
              .toLowerCase()
              .contains("the oracle \"thin\" jdbc driver is not being used."));
    }

    // Incorrect driver-type (i.e. not using the "thin" driver)...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:loremipsum:@hostname.domain.com.au:port1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      fail(
          "A JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      // This is what we want to happen.
      assertTrue(
          "An exception should be thown that refers to the fact that the thin "
          + "JDBC driver is not being used.",
          ex.getMessage().toLowerCase().contains(
              "oracle \"thin\" jdbc driver is not being used"));

      assertTrue(
          "An exception should be thown that tells us which JDBC driver "
          + "was specified.",
          ex.getMessage().toLowerCase().contains("loremipsum"));

    }

    // Invalid JDBC URL (unparsable port number)...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:thin:@hostname.domain.com.au:port1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      fail(
          "An JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      assertTrue(
         "The invalid port number should be included in the exception message.",
         ex.getMessage().toLowerCase().contains("port1521"));
    }

    // Invalid JDBC URL (negative port number)...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:thin:@hostname.domain.com.au:-1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      fail(
          "An JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      assertTrue(
         "The invalid port number should be included in the exception message.",
         ex.getMessage().toLowerCase().contains("-1521"));
    }

    // Valid JDBC URL...
    try {
      actual =
          new OraOopJdbcUrl(
              "JDBC:Oracle:tHiN:@hostname.domain.com.au:1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname.domain.com.au", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals("dbsid", actual.getSid());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid JDBC URL...
    try {
      actual =
          new OraOopJdbcUrl(
              " JDBC : Oracle : tHiN : @hostname.domain.com.au : 1529 : dbsid")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname.domain.com.au", actual.getHost());
      assertEquals(1529, actual.getPort());
      assertEquals("dbsid", actual.getSid());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid (sid-based) JDBC URL with parameters...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:thin:@hostname:1521:dbsid?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals("dbsid", actual.getSid());
      assertEquals(null, actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:thin:@hostname:1521/dbservice.dbdomain")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals(null, actual.getSid());
      assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with slashes...
    try {
      actual =
          new OraOopJdbcUrl(
              "jdbc:oracle:thin:@//hostname:1521/dbservice.dbdomain")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals(null, actual.getSid());
      assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with parameters...
    try {
      actual = new OraOopJdbcUrl(
         "jdbc:oracle:thin:@hostname:1521/dbservice.dbdomain?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals(null, actual.getSid());
      assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with slashes and parameters...
    try {
      actual = new OraOopJdbcUrl(
       "jdbc:oracle:thin:@//hostname:1521/dbservice.dbdomain?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      assertEquals("hostname", actual.getHost());
      assertEquals(1521, actual.getPort());
      assertEquals(null, actual.getSid());
      assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      fail(ex.getMessage());
    }
  }

  @Test
  public void testGetConnectionUrl() {

    String actual;

    // Null JDBC URL...
    try {
      actual = new OraOopJdbcUrl(null).getConnectionUrl();
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    }

    // Empty JDBC URL...
    try {
      actual = new OraOopJdbcUrl("").getConnectionUrl();
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    }

    // JDBC URL...
    actual =
        new OraOopJdbcUrl("jdbc:oracle:thin:@hostname.domain:1521:dbsid")
            .getConnectionUrl();
    assertEquals("jdbc:oracle:thin:@hostname.domain:1521:dbsid", actual);

  }

}
