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
 * Static helper class that will help format data with quotes and escape chars.
 *
 * @deprecated use org.apache.sqoop.lib.FieldFormatter instead.
 * @see org.apache.sqoop.lib.FieldFormatter
 */
public final class FieldFormatter {

  private FieldFormatter() { }

  /**
   * only pass fields that are strings when --hive-drop-delims option is on.
   * @param str
   * @param delimiters
   * @return
   */
  public static String hiveStringDropDelims(String str,
          DelimiterSet delimiters) {
    return org.apache.sqoop.lib.FieldFormatter.hiveStringDropDelims(
        str, delimiters);
  }

  /**
   * replace hive delimiters with a user-defined string passed to the
   * --hive-delims-replacement option.
   * @param str
   * @param delimiters
   * @return
   */
  public static String hiveStringReplaceDelims(String str, String replacement,
      DelimiterSet delimiters) {
    return org.apache.sqoop.lib.FieldFormatter.hiveStringReplaceDelims(
        str, replacement, delimiters);
  }

  /**
   * Takes an input string representing the value of a field, encloses it in
   * enclosing chars, and escapes any occurrences of such characters in the
   * middle.  The escape character itself is also escaped if it appears in the
   * text of the field.  If there is no enclosing character, then any
   * delimiters present in the field body are escaped instead.
   *
   * The field is enclosed only if:
   *   enclose != '\000', and:
   *     encloseRequired is true, or
   *     one of the fields-terminated-by or lines-terminated-by characters is
   *     present in the string.
   *
   * Escaping is not performed if the escape char is '\000'.
   *
   * @param str - The user's string to escape and enclose
   * @param delimiters - The DelimiterSet to use identifying the escape and
   * enclose semantics. If the specified escape or enclose characters are
   * '\000', those operations are not performed.
   * @return the escaped, enclosed version of 'str'.
   */
  public static String escapeAndEnclose(String str, DelimiterSet delimiters) {
    return org.apache.sqoop.lib.FieldFormatter.escapeAndEnclose(
        str, delimiters);
  }
}
