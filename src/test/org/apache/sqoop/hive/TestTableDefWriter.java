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
package org.apache.sqoop.hive;

import static org.mockito.Mockito.*;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.SqoopOptions;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.*;
import java.util.HashMap;
import java.io.IOException;

public class TestTableDefWriter {
  static String inputTableName = "genres";
  static String outputTableName = "genres";
  static String testTargetDir = "/tmp/testDB/genre";
  static String hdfsTableDir = "/data/movielens/genre";
  static String testDbUri = "jdbc:postgresql://localhost/movielens";
  static ConnManager manager;
  static SqoopOptions options;
  public static final Log LOG = LogFactory.getLog(
      TestTableDefWriter.class.getName());
  TableDefWriter tableDefWriter;

  @BeforeClass
  public static void setup() {
    // create mock
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("id", Types.TINYINT);
    map.put("name", Types.VARCHAR);
    manager = Mockito.mock(ConnManager.class);
    when(manager.getColumnNames(inputTableName)).thenReturn(new String[] { "id", "name" });
    when(manager.getColumnTypes(inputTableName)).thenReturn(map);
    options = new SqoopOptions(testDbUri, inputTableName);
    options.setTargetDir(testTargetDir);
    options.setHiveExternalTableDir(hdfsTableDir);
    String[] cols = new String[] { "id", "name" };
    options.setColumns(cols);
    options.setMapColumnHive("id=TINYINT,name=STRING");
  }

  @Test
  public void testGenerateExternalTableStatement() throws IOException, SQLException {
    // need to set this as the other unit test functions may override it for their own test.
    options.setHiveExternalTableDir(hdfsTableDir);
    tableDefWriter = new TableDefWriter(options, manager, inputTableName, outputTableName,
        options.getConf(), false);
    String stmt = tableDefWriter.getCreateTableStmt();
    Boolean isHiveExternalTableSet = !StringUtils.isBlank(options.getHiveExternalTableDir());
    LOG.debug("External table dir: "+options.getHiveExternalTableDir());
    assert (isHiveExternalTableSet && stmt.contains("CREATE EXTERNAL TABLE ") && stmt.contains("LOCATION '" + hdfsTableDir));
  }

  @Test
  public void testGenerateTableStatement() throws IOException, SQLException {
    // need to set this as the other unit test functions may override it for their own test.
    options.setHiveExternalTableDir(null);
    tableDefWriter = new TableDefWriter(options, manager, inputTableName, outputTableName,
        options.getConf(), false);
    String stmt = tableDefWriter.getCreateTableStmt();
    Boolean isHiveExternalTableSet = !StringUtils.isBlank(options.getHiveExternalTableDir());
    LOG.debug("External table dir: "+options.getHiveExternalTableDir());
    assert (!isHiveExternalTableSet && stmt.contains("CREATE TABLE "));
  }

  @Test
  public void testGenerateExternalTableIfExistsStatement() throws IOException, SQLException {
    options.setFailIfHiveTableExists(false);
    // need to set this as the other unit test functions may override it for their own test.
    options.setHiveExternalTableDir(hdfsTableDir);
    tableDefWriter = new TableDefWriter(options, manager, inputTableName, outputTableName,
        options.getConf(), false);
    String stmt = tableDefWriter.getCreateTableStmt();
    Boolean isHiveExternalTableSet = !StringUtils.isBlank(options.getHiveExternalTableDir());
    LOG.debug("External table dir: "+options.getHiveExternalTableDir());
    assert (isHiveExternalTableSet && stmt.contains("CREATE EXTERNAL TABLE IF NOT EXISTS") && stmt.contains("LOCATION '"
        + hdfsTableDir));
  }

  @Test
  public void testGenerateTableIfExistsStatement() throws IOException, SQLException {
    // need to set this as the other unit test functions may override it for their own test.
    options.setHiveExternalTableDir(null);
    tableDefWriter = new TableDefWriter(options, manager, inputTableName, outputTableName,
        options.getConf(), false);
    String stmt = tableDefWriter.getCreateTableStmt();
    Boolean isHiveExternalTableSet = !StringUtils.isBlank(options.getHiveExternalTableDir());
    LOG.debug("External table dir: "+options.getHiveExternalTableDir());
    assert (!isHiveExternalTableSet && stmt.contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test
  public void testGenerateExternalTableLoadStatement() throws IOException, SQLException {
    // need to set this as the other unit test functions may override it for their own test.
    options.setHiveExternalTableDir(hdfsTableDir);
    tableDefWriter = new TableDefWriter(options, manager, inputTableName, outputTableName,
        options.getConf(), false);
    String stmt = tableDefWriter.getLoadDataStmt();
    Boolean isHiveExternalTableSet = !StringUtils.isBlank(options.getHiveExternalTableDir());
    LOG.debug("External table dir: "+options.getHiveExternalTableDir());
    assert (isHiveExternalTableSet && stmt.contains("LOAD DATA INPATH ") && stmt.contains(testTargetDir));
  }
}
