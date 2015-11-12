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

/**
 * Set of constants specifically for phoenix. 
 */
public final class PhoenixConstants {

	/** property used to specify the column mapping of db to phoenix **/
	public static final String PHOENIX_COLUMN_MAPPING = "sqoop.phoenix.import.column.mapping";
	
	/** property used to specify the columns beings imported from sqoop. */
	public static final String PHOENIX_SQOOP_COLUMNS = "sqoop.phoenix.columns";
	
	/** separator for phoenix columns */
	public static final String PHOENIX_COLUMN_MAPPING_SEPARATOR = ",";
	
	/** separator between phoenix and sqoop column.  */
	public static final String PHOENIX_SQOOP_COLUMN_SEPARATOR = ";";
}
