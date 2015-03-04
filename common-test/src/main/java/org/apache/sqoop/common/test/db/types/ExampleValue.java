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
package org.apache.sqoop.common.test.db.types;

/**
 * Example value with different representations.
 */
public class ExampleValue {
  /**
   * Properly escaped value so that it can be used in INSERT statement.
   */
  public final String insertStatement;

  /**
   * Object value that should be returned from JDBC driver getObject().
   */
  public final Object objectValue;

  /**
   * Escaped string value that will be stored by HDFS connector.
   */
  public final String escapedStringValue;

  public ExampleValue(String insertStatement, Object objectValue, String escapedStringValue) {
    this.insertStatement = insertStatement;
    this.objectValue = objectValue;
    this.escapedStringValue = escapedStringValue;
  }
}
