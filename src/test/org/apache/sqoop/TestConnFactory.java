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

package org.apache.sqoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.manager.ManagerFactory;
import org.apache.sqoop.metastore.JobData;
import org.apache.sqoop.tool.ImportTool;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Test the ConnFactory implementation and its ability to delegate to multiple
 * different ManagerFactory implementations using reflection.
 */

public class TestConnFactory {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Test
  public void testCustomFactory() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY,
        AlwaysDummyFactory.class.getName());

    ConnFactory factory = new ConnFactory(conf);
    ConnManager manager = factory.getManager(
        new JobData(new SqoopOptions(), new ImportTool()));
    assertNotNull("No manager returned", manager);
    assertTrue("Expected a DummyManager", manager instanceof DummyManager);
  }

  @Test

  public void testExceptionForNoManager() throws IOException {
    Configuration conf = new Configuration();
    conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY, EmptyFactory.class.getName());

    ConnFactory factory = new ConnFactory(conf);

    thrown.expect(IOException.class);
    thrown.reportMissingExceptionWithMessage("Expected IOException because of missing ConnManager ");
    factory.getManager(
        new JobData(new SqoopOptions(), new ImportTool()));
  }

  @Test
  public void testMultipleManagers() throws IOException {
    Configuration conf = new Configuration();
    // The AlwaysDummyFactory is second in this list. Nevertheless, since
    // we know the first factory in the list will return null, we should still
    // get a DummyManager out.
    String classNames = EmptyFactory.class.getName()
        + "," + AlwaysDummyFactory.class.getName();
    conf.set(ConnFactory.FACTORY_CLASS_NAMES_KEY, classNames);

    ConnFactory factory = new ConnFactory(conf);
    ConnManager manager = factory.getManager(
        new JobData(new SqoopOptions(), new ImportTool()));
    assertNotNull("No manager returned", manager);
    assertTrue("Expected a DummyManager", manager instanceof DummyManager);
  }

  ////// mock classes used for test cases above //////

  /**
   * Factory that always returns a DummyManager, regardless of the
   * configuration.
   */
  public static class AlwaysDummyFactory extends ManagerFactory {
    public ConnManager accept(JobData data) {
      // Always return a new DummyManager
      return new DummyManager();
    }
  }

  /**
   * ManagerFactory that accepts no configurations.
   */
  public static class EmptyFactory extends ManagerFactory {
    public ConnManager accept(JobData data) {
      // Never instantiate a proper ConnManager;
      return null;
    }
  }

  /**
   * This implementation doesn't do anything special.
   */
  public static class DummyManager extends ConnManager {
    public void close() {
    }

    public String [] listDatabases() {
      return null;
    }

    public String [] listTables() {
      return null;
    }

    public String [] getColumnNames(String tableName) {
      return null;
    }

    public String[] getColumnNamesForProcedure(String procedureName) {
      return null;
    }

    public String getPrimaryKey(String tableName) {
      return null;
    }

    public Map<String,List<Integer>> getColumnInfo(String tableName) {
      return null;
    }

    /**
    * Default implementation.
    * @param sqlType     sql data type
    * @return            java data type
    */
    public String toJavaType(int sqlType) {
      return null;
    }

    /**
    * Default implementation.
    * @param sqlType     sql data type
    * @return            hive data type
    */
    public String toHiveType(int sqlType) {
      return null;
    }

    public Map<String, Integer> getColumnTypes(String tableName) {
      return null;
    }

    @Override
    public Map<String, Integer> getColumnTypesForProcedure(
        String procedureName) {
      return null;
    }

    public ResultSet readTable(String tableName, String [] columns) {
      return null;
    }

    public Connection getConnection() {
      return null;
    }

    public String getDriverClass() {
      return null;
    }

    public void execAndPrint(String s) {
    }

    public void importTable(ImportJobContext context) {
    }

    public void release() {
    }
  }

}
