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

package com.cloudera.sqoop.phoenix;

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

/**
 * 
 * Tests import to phoenix tables.
 *
 */
public class PhoenixImportTest extends PhoenixTestCase {
	
	@Test
	public void testBasicUsageWithNoColumnMapping() throws Exception {
	    String tableName = "TABLE1";
	    String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER)" , tableName);
	    createTestTable(ddl);
		String [] argv = getArgv(true, tableName, null, null);
		String [] columnNames = { "ID" , "NAME" , "AGE"};
	    String [] colTypes = { "INT", "VARCHAR(32)" , "INT"};
	    String [] vals = { "0", "Name 1" , "1" };
	    createTableWithColTypesAndNames(columnNames, colTypes, vals);
	    runImport(argv);
	    
	    //verify the data.
	    Statement stmt = conn.createStatement();
	    ResultSet rs = stmt.executeQuery(String.format("SELECT id, name, age FROM %s",tableName));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertEquals(1, rs.getInt(3));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
	}
	
	@Test
	public void testBasicUsageWithColumnMapping() throws Exception {
	    String tableName = "TABLE2";
	    String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER)" , tableName);
	    createTestTable(ddl);
		String [] argv = getArgv(true, tableName, "rowid;ID,name;NAME,age;AGE", null);
		String [] columnNames = { "rowid" , "name" , "age"};
	    String [] colTypes = { "INT", "VARCHAR(32)" , "INT"};
	    String [] vals = { "0", "Name 1" , "1" };
	    createTableWithColTypesAndNames(columnNames, colTypes, vals);
	    runImport(argv);
	    
	    //verify the data.
	    Statement stmt = conn.createStatement();
	    ResultSet rs = stmt.executeQuery(String.format("SELECT id, name, age FROM %s",tableName));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Name 1", rs.getString(2));
        assertEquals(1, rs.getInt(3));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
	}

}
