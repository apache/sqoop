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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Utility class  .
 *
 */
public class PhoenixUtil {

	public static final Log LOG = LogFactory.getLog(
			PhoenixUtil.class.getName());
	
	private static boolean testingMode = false;
	
	private PhoenixUtil() { 
	}

	/**
	 * This is a way to make this always return false for testing.
	 */
	public static void setAlwaysNoPhoenixJarMode(boolean mode) {
		testingMode = mode;
	}

	public static boolean isPhoenixJarPresent() {
	  if (testingMode) {
	  	return false;
	  }
	  try {
		  // validate if hbase jars also exist in classpath.
	   	Class.forName("org.apache.hadoop.hbase.client.HTable");
	   	Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
	  } catch (ClassNotFoundException cnfe) {
	  	LOG.error("Failed to find phoenix dependencies in classpath : " + cnfe.getMessage());
	  	return false;
	  }
	  return true;
	}
	
	/**
	 * Generates a map of phoenix_column to sqoop column.
	 * @param columnMapping
	 * @return
	 */
	public static Map<String,String> getPhoenixToSqoopMap(String columnMappings) {
		String[] split = columnMappings.split(PhoenixConstants.PHOENIX_COLUMN_MAPPING_SEPARATOR);
		Map<String,String> columnMappingsMap = new HashMap<String,String>();
		for (String each : split) {
			String[] sqoopToPhoenixMapping = each.split(PhoenixConstants.PHOENIX_SQOOP_COLUMN_SEPARATOR);
			// if the sqoop column name is the same as phoenix column name, 
			// we don't need to separate the columns by a ';' delimiter.
			if (sqoopToPhoenixMapping.length == 2) {
				columnMappingsMap.put(sqoopToPhoenixMapping[1], sqoopToPhoenixMapping[0]);				
			} else {
				columnMappingsMap.put(sqoopToPhoenixMapping[0].toUpperCase(), sqoopToPhoenixMapping[0]);
			}
		}
		return columnMappingsMap;
	}

	/**
	 * does the following validations
	 * 1. count of columns in sqoop match phoenix
	 * 2. 1 to 1 mapping between sqoop column to phoenix column.
	 * @param columnNames
	 * @param phoenixColumnMappings
	 */
	public static boolean validateColumns(String sColumns, String columnMappings) {
		Map<String,String> phoenixToSqoopColumnMap = getPhoenixToSqoopMap(columnMappings);
		String sqoopColumns[] = sColumns.split(",");
		if (sqoopColumns.length != phoenixToSqoopColumnMap.size()) {
			throw new RuntimeException("Mismatch in the number of columns being imported from Sqoop "
				+ "and written to phoenix.");
		}
		Collection<String> values = phoenixToSqoopColumnMap.values();
		for (String sqoopColumn : sqoopColumns) {
			if (!values.contains(sqoopColumn)) {
				throw new RuntimeException(String.format("Sqoop column [%s] doesn't exist in the valid list"
					+ " of column mappings [%s] ",sqoopColumn, Arrays.toString(values.toArray())));
			}
		}
		return true;
	}
 }
