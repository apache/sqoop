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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for OraOopUtilities.
 */
public class TestOraOopUtilities extends OraOopTestCase {

  @Test
  public void testdecodeOracleTableName() {

    OracleTable context = null;

    // These are the possibilities for double-quote location...
    // table
    // "table"
    // schema.table
    // schema."table"
    // "schema".table
    // "schema"."table"

    // table
    context = OraOopUtilities.decodeOracleTableName("oraoop", "junk", null);
    assertEquals(context.getSchema(), "ORAOOP");
    assertEquals(context.getName(), "JUNK");

    // "table"
    context = OraOopUtilities.decodeOracleTableName("oraoop", "\"Junk\"", null);
    assertEquals(context.getSchema(), "ORAOOP");
    assertEquals(context.getName(), "Junk");

    // schema.table
    context =
        OraOopUtilities.decodeOracleTableName("oraoop", "targusr.junk", null);
    assertEquals(context.getSchema(), "TARGUSR");
    assertEquals(context.getName(), "JUNK");

    // schema."table"
    context =
        OraOopUtilities.decodeOracleTableName("oraoop", "targusr.\"Junk\"",
            null);
    assertEquals(context.getSchema(), "TARGUSR");
    assertEquals(context.getName(), "Junk");

    // "schema".table
    context =
        OraOopUtilities.decodeOracleTableName("oraoop", "\"Targusr\".junk",
            null);
    assertEquals(context.getSchema(), "Targusr");
    assertEquals(context.getName(), "JUNK");

    // "schema"."table"
    String inputStr = "\"Targusr\".\"Junk\"";
    context = OraOopUtilities.decodeOracleTableName("oraoop", inputStr, null);
    assertEquals(context.getSchema(), "Targusr");
    assertEquals(context.getName(), "Junk");

    // Test for "." within schema...
    context =
        OraOopUtilities.decodeOracleTableName("oraoop", "\"targ.usr\".junk",
            null);
    assertEquals(context.getSchema(), "targ.usr");
    assertEquals(context.getName(), "JUNK");

    // Test for "." within table...
    context =
        OraOopUtilities.decodeOracleTableName("oraoop",
            "targusr.\"junk.tab.with.dots\"", null);
    assertEquals(context.getSchema(), "TARGUSR");
    assertEquals(context.getName(), "junk.tab.with.dots");

    // Test for "." within schema and within table...
    context =
        OraOopUtilities.decodeOracleTableName("oraoop",
            "\"targ.usr\".\"junk.tab.with.dots\"", null);
    assertEquals(context.getSchema(), "targ.usr");
    assertEquals(context.getName(), "junk.tab.with.dots");
  }

  @Test
  public void testgetCurrentMethodName() {

    String actual = OraOopUtilities.getCurrentMethodName();
    String expected = "testgetCurrentMethodName()";

    assertEquals(expected, actual);

  }

  @Test
  public void testgenerateDataChunkId() {

    String expected;
    String actual;

    expected = "1_1";
    actual = OraOopUtilities.generateDataChunkId(1, 1);
    assertEquals(expected, actual);

    expected = "1234_99";
    actual = OraOopUtilities.generateDataChunkId(1234, 99);
    assertEquals(expected, actual);
  }

  @Test
  public void testgetDuplicatedStringArrayValues() {

    try {
      OraOopUtilities.getDuplicatedStringArrayValues(null, false);
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      // This is what we want to happen.
    }

    String[] duplicates = null;

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] {}, false);
    assertEquals(0, duplicates.length);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "a", "b",
            "c", }, false);
    assertEquals(0, duplicates.length);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "a", "A",
            "b", }, false);
    assertEquals(0, duplicates.length);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "a", "A",
            "b", }, true);
    assertEquals(1, duplicates.length);
    assertEquals("A", duplicates[0]);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "A", "a",
            "b", }, true);
    assertEquals(1, duplicates.length);
    assertEquals("a", duplicates[0]);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "A", "a",
            "b", "A", }, false);
    assertEquals(1, duplicates.length);
    assertEquals("A", duplicates[0]);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "A", "a",
            "b", "A", }, true);
    assertEquals(2, duplicates.length);
    assertEquals("a", duplicates[0]);
    assertEquals("A", duplicates[1]);

    duplicates =
        OraOopUtilities.getDuplicatedStringArrayValues(new String[] { "A", "a",
            "b", "A", "A", }, true);
    assertEquals(2, duplicates.length);
    assertEquals("a", duplicates[0]);
    assertEquals("A", duplicates[1]);
  }

  @Test
  public void testgetFullExceptionMessage() {

    try {

      try {
        try {
          throw new IOException("lorem ipsum!");
        } catch (IOException ex) {
          throw new SQLException("dolor sit amet", ex);
        }
      } catch (SQLException ex) {
        throw new RuntimeException("consectetur adipisicing elit", ex);
      }

    } catch (Exception ex) {
      String msg = OraOopUtilities.getFullExceptionMessage(ex);
      if (!msg.contains("IOException") || !msg.contains("lorem ipsum!")) {
        fail("Inner exception text has not been included in the message");
      }
      if (!msg.contains("SQLException") || !msg.contains("dolor sit amet")) {
        fail("Inner exception text has not been included in the message");
      }
      if (!msg.contains("RuntimeException")
          || !msg.contains("consectetur adipisicing elit")) {
        fail("Outer exception text has not been included in the message");
      }
    }
  }

  @Test
  public void testGetOraOopOracleDataChunkMethod() {
    try {
      OraOopUtilities.getOraOopOracleDataChunkMethod(null);
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      // This is what we want to happen.
    }

    OraOopConstants.OraOopOracleDataChunkMethod dataChunkMethod;
    Configuration conf = new Configuration();

    // Check the default is ROWID
    dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
    assertEquals(OraOopConstants.OraOopOracleDataChunkMethod.ROWID,
        dataChunkMethod);

    // Invalid value specified
    OraOopUtilities.LOG.setCacheLogEntries(true);
    OraOopUtilities.LOG.clearCache();
    conf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, "loremipsum");
    dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
    String logText = OraOopUtilities.LOG.getLogEntries();
    OraOopUtilities.LOG.setCacheLogEntries(false);
    if (!logText.toLowerCase().contains("loremipsum")) {
      fail("The LOG should inform the user they've selected an invalid "
              + "data chunk method - and what that was.");
    }
    assertEquals("Should have used the default value",
        OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD_DEFAULT,
        dataChunkMethod);

    // Valid value specified
    conf.set(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD, "partition");
    dataChunkMethod = OraOopUtilities.getOraOopOracleDataChunkMethod(conf);
    assertEquals(OraOopConstants.OraOopOracleDataChunkMethod.PARTITION,
        dataChunkMethod);
  }

  @Test
  public void testgetOraOopOracleBlockToSplitAllocationMethod() {

    // Invalid arguments test...
    try {
      OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(null,
          OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM);
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      // This is what we want to happen.
    }

    OraOopConstants.OraOopOracleBlockToSplitAllocationMethod allocationMethod;
    org.apache.hadoop.conf.Configuration conf = new Configuration();

    // No configuration property - and RANDOM used by default...
    allocationMethod =
        OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(conf,
            OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM);
    assertEquals(
        OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.RANDOM,
        allocationMethod);

    // No configuration property - and SEQUENTIAL used by default...
    allocationMethod =
        OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(
           conf,
           OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
    assertEquals(
        OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL,
        allocationMethod);

    // An invalid property value specified...
    OraOopUtilities.LOG.setCacheLogEntries(true);
    OraOopUtilities.LOG.clearCache();
    conf.set(OraOopConstants.ORAOOP_ORACLE_BLOCK_TO_SPLIT_ALLOCATION_METHOD,
        "loremipsum");
    allocationMethod =
        OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(
           conf,
           OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
    String logText = OraOopUtilities.LOG.getLogEntries();
    OraOopUtilities.LOG.setCacheLogEntries(false);
    if (!logText.toLowerCase().contains("loremipsum")) {
      fail("The LOG should inform the user they've selected an invalid "
              + "allocation method - and what that was.");
    }

    if (!logText.contains("ROUNDROBIN or SEQUENTIAL or RANDOM")) {
      fail("The LOG should inform the user what the valid choices are.");
    }

    // An valid property value specified...
    conf.set(OraOopConstants.ORAOOP_ORACLE_BLOCK_TO_SPLIT_ALLOCATION_METHOD,
        "sequential");
    allocationMethod =
        OraOopUtilities.getOraOopOracleBlockToSplitAllocationMethod(
           conf,
           OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);
    assertEquals(
        OraOopConstants.OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL,
        allocationMethod);
  }

  @Test
  public void testgetOraOopTableImportWhereClauseLocation() {

    // Invalid arguments test...
    try {
      OraOopUtilities.getOraOopTableImportWhereClauseLocation(null,
          OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
      fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      // This is what we want to happen.
    }

    OraOopConstants.OraOopTableImportWhereClauseLocation location;
    org.apache.hadoop.conf.Configuration conf = new Configuration();

    // No configuration property - and SPLIT used by default...
    location =
        OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf,
            OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
    assertEquals(
        OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT, location);

    // An invalid property value specified...
    OraOopUtilities.LOG.setCacheLogEntries(true);
    OraOopUtilities.LOG.clearCache();
    conf.set(OraOopConstants.ORAOOP_TABLE_IMPORT_WHERE_CLAUSE_LOCATION,
        "loremipsum");
    location =
        OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf,
            OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT);
    String logText = OraOopUtilities.LOG.getLogEntries();
    OraOopUtilities.LOG.setCacheLogEntries(false);
    if (!logText.toLowerCase().contains("loremipsum")) {
      fail("The LOG should inform the user they've selected an invalid "
              + "where-clause-location - and what that was.");
    }

    if (!logText.contains("SUBSPLIT or SPLIT")) {
      fail("The LOG should inform the user what the valid choices are.");
    }

    // An valid property value specified...
    conf.set(OraOopConstants.ORAOOP_TABLE_IMPORT_WHERE_CLAUSE_LOCATION,
        "split");
    location =
        OraOopUtilities.getOraOopTableImportWhereClauseLocation(conf,
            OraOopConstants.OraOopTableImportWhereClauseLocation.SUBSPLIT);
    assertEquals(
        OraOopConstants.OraOopTableImportWhereClauseLocation.SPLIT, location);

  }

  @Test
  public void testpadLeft() {

    String expected = "   a";
    String actual = OraOopUtilities.padLeft("a", 4);
    assertEquals(expected, actual);

    expected = "abcd";
    actual = OraOopUtilities.padLeft("abcd", 3);
    assertEquals(expected, actual);
  }

  @Test
  public void testpadRight() {

    String expected = "a   ";
    String actual = OraOopUtilities.padRight("a", 4);
    assertEquals(expected, actual);

    expected = "abcd";
    actual = OraOopUtilities.padRight("abcd", 3);
    assertEquals(expected, actual);
  }

  @Test
  public void testReplaceConfigurationExpression() {

    org.apache.hadoop.conf.Configuration conf = new Configuration();

    // Default value used...
    String actual =
        OraOopUtilities.replaceConfigurationExpression(
            "alter session set timezone = '{oracle.sessionTimeZone|GMT}';",
            conf);
    String expected = "alter session set timezone = 'GMT';";
    assertEquals("OraOop configuration expression failure.", expected,
        actual);

    // Configuration property value exists...
    conf.set("oracle.sessionTimeZone", "Africa/Algiers");
    actual =
        OraOopUtilities.replaceConfigurationExpression(
            "alter session set timezone = '{oracle.sessionTimeZone|GMT}';",
            conf);
    expected = "alter session set timezone = 'Africa/Algiers';";
    assertEquals("OraOop configuration expression failure.", expected,
        actual);

    // Multiple properties in one expression...
    conf.set("expr1", "1");
    conf.set("expr2", "2");
    conf.set("expr3", "3");
    conf.set("expr4", "4");
    actual =
        OraOopUtilities.replaceConfigurationExpression("set {expr1}={expr2};",
            conf);
    expected = "set 1=2;";
    assertEquals("OraOop configuration expression failure.", expected,
        actual);

    actual =
        OraOopUtilities.replaceConfigurationExpression(
            "set {expr4|0}={expr5|5};", conf);
    expected = "set 4=5;";
    assertEquals("OraOop configuration expression failure.", expected,
        actual);
  }

  @Test
  public void testStackContainsClass() {

    if (OraOopUtilities.stackContainsClass("lorem.ipsum.dolor")) {
      fail("There's no way the stack actually contains this!");
    }

    String expected = "org.apache.sqoop.manager.oracle.TestOraOopUtilities";
    if (!OraOopUtilities.stackContainsClass(expected)) {
      fail("The stack should contain the class:" + expected);
    }
  }

  @Test
  public void testGetImportHint() {
    org.apache.hadoop.conf.Configuration conf = new Configuration();

    String hint = OraOopUtilities.getImportHint(conf);
    assertEquals("Default import hint", "/*+ NO_INDEX(t) */ ", hint);

    conf.set("oraoop.import.hint", "NO_INDEX(t) SCN_ASCENDING");
    hint = OraOopUtilities.getImportHint(conf);
    assertEquals("Changed import hint",
        "/*+ NO_INDEX(t) SCN_ASCENDING */ ", hint);

    conf.set("oraoop.import.hint", "       ");
    hint = OraOopUtilities.getImportHint(conf);
    assertEquals("Whitespace import hint", "", hint);

    conf.set("oraoop.import.hint", "");
    hint = OraOopUtilities.getImportHint(conf);
    assertEquals("Blank import hint", "", hint);

  }

  @Test
  public void testSplitStringList() {
    List<String> result = null;
    List<String> expected = null;

    expected = new ArrayList<String>();
    expected.add("abcde");
    expected.add("ghijklm");
    result = OraOopUtilities.splitStringList("abcde,ghijklm");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("\"abcde\"");
    expected.add("\"ghijklm\"");
    result = OraOopUtilities.splitStringList("\"abcde\",\"ghijklm\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("abcde");
    expected.add("\"ghijklm\"");
    result = OraOopUtilities.splitStringList("abcde,\"ghijklm\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("\"abcde\"");
    expected.add("ghijklm");
    result = OraOopUtilities.splitStringList("\"abcde\",ghijklm");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("\"ab,cde\"");
    expected.add("ghijklm");
    result = OraOopUtilities.splitStringList("\"ab,cde\",ghijklm");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("abcde");
    expected.add("\"ghi,jklm\"");
    result = OraOopUtilities.splitStringList("abcde,\"ghi,jklm\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("\"ab,cde\"");
    expected.add("\"ghi,jklm\"");
    result = OraOopUtilities.splitStringList("\"ab,cde\",\"ghi,jklm\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("\"ab,cde\"");
    expected.add("\"ghi,jklm\"");
    expected.add("\",Lorem\"");
    expected.add("\"ip!~sum\"");
    expected.add("\"do,lo,,r\"");
    expected.add("\"s#it\"");
    expected.add("\"am$e$t\"");
    result =
        OraOopUtilities
            .splitStringList("\"ab,cde\",\"ghi,jklm\",\",Lorem\",\"ip!~sum\","
                + "\"do,lo,,r\",\"s#it\",\"am$e$t\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("LOREM");
    expected.add("IPSUM");
    expected.add("DOLOR");
    expected.add("SIT");
    expected.add("AMET");
    result = OraOopUtilities.splitStringList("LOREM,IPSUM,DOLOR,SIT,AMET");
    assertEquals(expected, result);
  }

  @Test
  public void testSplitOracleStringList() {
    List<String> result = null;
    List<String> expected = null;

    expected = new ArrayList<String>();
    expected.add("LOREM");
    expected.add("IPSUM");
    expected.add("DOLOR");
    expected.add("SIT");
    expected.add("AMET");
    result =
        OraOopUtilities.splitOracleStringList("lorem,ipsum,dolor,sit,amet");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("LOREM");
    expected.add("ipsum");
    expected.add("dolor");
    expected.add("SIT");
    expected.add("amet");
    result =
        OraOopUtilities
            .splitOracleStringList("lorem,\"ipsum\",\"dolor\",sit,\"amet\"");
    assertEquals(expected, result);

    expected = new ArrayList<String>();
    expected.add("LOREM");
    expected.add("ip,sum");
    expected.add("dol$or");
    expected.add("SIT");
    expected.add("am!~#et");
    result =
        OraOopUtilities
          .splitOracleStringList("lorem,\"ip,sum\",\"dol$or\",sit,\"am!~#et\"");
    assertEquals(expected, result);
  }

  @Test
  public void testAppendJavaSecurityEgd() {
    String confProperty = "mapred.child.java.opts";
    String confValue = "-Djava.security.egd=file:///dev/urandom";
    Configuration conf = new Configuration();

    String expected = confValue;
    String actual = null;
    conf.set(confProperty, "");
    OraOopUtilities.appendJavaSecurityEgd(conf);
    actual = conf.get(confProperty);
    assertEquals("Append to empty string", expected, actual);

    expected = "-Djava.security.egd=file:/dev/random";
    conf.set(confProperty, expected);
    OraOopUtilities.appendJavaSecurityEgd(conf);
    actual = conf.get(confProperty);
    assertEquals("Append to empty string", expected, actual);

    expected = confValue + " -Xmx201m";
    conf.set(confProperty, "-Xmx201m");
    OraOopUtilities.appendJavaSecurityEgd(conf);
    actual = conf.get(confProperty);
    assertEquals("Append to empty string", expected, actual);
  }
}
