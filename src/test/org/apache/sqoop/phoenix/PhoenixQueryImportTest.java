package org.apache.sqoop.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

/**
 * Tests to insert onto phoenix table using query 
 *
 */
public class PhoenixQueryImportTest extends PhoenixBaseTestCase {

	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testQueryImport() throws Exception {
		Connection phoenixConnection = null;
		ResultSet rs = null;
    try {
    	
      // create phoenix table
      final String tableName = "TABLE1";
	    final String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, LOCATION VARCHAR)" , tableName);
	    createPhoenixTestTable(ddl);
	    
	    // create sqoop table
	    String [] columnNames = { "ID" , "NAME" , "LOCATION"};
	    String [] colTypes = { "INT", "VARCHAR(32)" , "VARCHAR(32)"};
	    String [] vals = { "1", "'first'" , "'CA'" };
	    createTableWithColTypesAndNames(columnNames, colTypes, vals);
	    
	    // run import
	    String sqoopTableName = getTableName();
	    String query = String.format("SELECT id AS ID, name AS NAME FROM %s WHERE $CONDITIONS",sqoopTableName);
	    String [] argv = getArgv(true, "ID", tableName, null, false ,query);
	    runImport(argv);
	    
	    //verify the data.
	    Statement pstmt = getPhoenixConnection().createStatement();
	    rs = pstmt.executeQuery(String.format("SELECT id, name,location FROM %s",tableName));
	    assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("first", rs.getString(2));
      assertEquals(null, rs.getString(3));
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
	
	@Test
	public void testInvalidArgument() {
		try {
			String [] columnNames = { "ID" , "NAME" , "LOCATION"};
			String [] colTypes = { "INT", "VARCHAR(32)" , "VARCHAR(32)"};
			String [] vals = { "1", "'first'" , "'CA'" };
			createTableWithColTypesAndNames(columnNames, colTypes, vals);
			String [] argv = getArgv(true, "ID", "SAMPLE", null, false , null);
			runImport(argv);
			fail("The test should fail as niether sqoop table nor sql query is passed.");
		} catch (Exception ex)  {
      return;
    } 
  }
}
