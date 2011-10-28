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
package com.cloudera.sqoop.lib;

/**
 * Encapsulates a set of delimiters used to encode a record.
 * @deprecated use org.apache.sqoop.lib.DelimiterSet instead.
 * @see org.apache.sqoop.lib.DelimiterSet
 */
public class DelimiterSet extends org.apache.sqoop.lib.DelimiterSet {

  public static final char NULL_CHAR =
      org.apache.sqoop.lib.DelimiterSet.NULL_CHAR;

  /**
   * Create a delimiter set with the default delimiters
   * (comma for fields, newline for records).
   */
  public DelimiterSet() {
    super();
  }

  /**
   * Create a delimiter set with the specified delimiters.
   * @param field the fields-terminated-by delimiter
   * @param record the lines-terminated-by delimiter
   * @param enclose the enclosed-by character
   * @param escape the escaped-by character
   * @param isEncloseRequired If true, enclosed-by is applied to all
   * fields. If false, only applied to fields that embed delimiters.
   */
  public DelimiterSet(char field, char record, char enclose, char escape,
      boolean isEncloseRequired) {
    super(field, record, enclose, escape, isEncloseRequired);
  }

  /**
   * Identical to clone() but does not throw spurious exceptions.
   * @return a new copy of this same set of delimiters.
   */
  public DelimiterSet copy() {
    try {
      return (DelimiterSet) clone();
    } catch (CloneNotSupportedException cnse) {
      // Should never happen for DelimiterSet.
      return null;
    }
  }

  // Static delimiter sets for the commonly-used delimiter arrangements.

  public static final DelimiterSet DEFAULT_DELIMITERS;
  public static final DelimiterSet HIVE_DELIMITERS;
  public static final DelimiterSet MYSQL_DELIMITERS;

  static {
    DEFAULT_DELIMITERS = new DelimiterSet(',', '\n', NULL_CHAR, NULL_CHAR,
        false);
    MYSQL_DELIMITERS = new DelimiterSet(',', '\n', '\'', '\\', false);
    HIVE_DELIMITERS = new DelimiterSet('\001', '\n',
        NULL_CHAR, NULL_CHAR, false);
  }
}

