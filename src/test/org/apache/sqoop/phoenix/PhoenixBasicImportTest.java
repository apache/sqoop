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

package org.apache.sqoop.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

/**
 * 
 * Tests to import onto phoenix tables.
 *
 */
public class PhoenixBasicImportTest extends PhoenixBaseTestCase {
	
	/**
	 * Test where the sqoop and phoenix table column names are the same.
	 * @throws Exception
	 */
	@Test
	public void testBasicUsageWithNoColumnMapping() throws Exception {
		Connection phoenixConnection = null;
		ResultSet rs = null;
    try {
			final String tableName = "TABLE1";
	    final String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER)" , tableName);
	    createPhoenixTestTable(ddl);
			   
			final String [] argv = getArgv(true, "ID", tableName, null, false, null);
			
			// create sqoop table
			String [] columnNames = { "ID" , "NAME" , "AGE"};
		  String [] colTypes = { "INT", "VARCHAR(32)" , "INT"};
		  String [] vals = { "0", "'first'" , "1" };
		  createTableWithColTypesAndNames(columnNames, colTypes, vals);
		  
		  // run the import.
		  runImport(argv);
	
		  // verify the result
		  phoenixConnection = getPhoenixConnection(); 
		  Statement stmt = phoenixConnection.createStatement();
		  rs = stmt.executeQuery(String.format("SELECT id, name, age FROM %s",tableName));
			assertTrue(rs.next());
	    assertEquals(0, rs.getInt(1));
	    assertEquals("first", rs.getString(2));
	    assertEquals(1, rs.getInt(3));
	    assertFalse(rs.next());
    } finally {
    	if(rs != null) {
    		rs.close();
    	}
    	if(phoenixConnection != null) {
    		phoenixConnection.close();
    	}
    }
	}
	
	/**
	 * Test where the sqoop table column names differ from phoenix table column name
	 * @throws Exception
	 */
	@Test
	public void testBasicUsageWithColumnMapping() throws Exception {
		Connection phoenixConnection = null;
		ResultSet rs = null;
    try {
    	// create phoenix table
		  final String tableName = "TABLE2";
		  final String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER)" , tableName);
		  createPhoenixTestTable(ddl);
		 
		  // create sqoop table
		  String [] columnNames = { "rowid" , "name" , "age"};
	    String [] colTypes = { "INT", "VARCHAR(32)" , "INT"};
	    String [] vals = { "0", "'Name 1'" , "1" };
	    createTableWithColTypesAndNames(columnNames, colTypes, vals);
	    
	    // run the import
	    String [] argv = getArgv(true, "rowid", tableName, "ROWID;ID,NAME,AGE", false, null);
	    runImport(argv);
	        
	    //verify the data.
	    phoenixConnection = getPhoenixConnection(); 
	    Statement stmt = phoenixConnection.createStatement();
	    rs = stmt.executeQuery(String.format("SELECT id, name, age FROM %s",tableName));
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
      assertEquals("Name 1", rs.getString(2));
      assertEquals(1, rs.getInt(3));
      assertFalse(rs.next());
    } finally {
    	if(rs != null) {
    		rs.close();
    	}
    	if(phoenixConnection != null) {
    		phoenixConnection.close();
    	}
    }
	}
}
