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
package org.apache.sqoop.lib;

import java.util.regex.Pattern;

/**
 * Static helper class that will help format data with quotes and escape chars.
 */
public final class FieldFormatter {

  private static final Pattern REPLACE_PATTERN = Pattern.compile("\\n|\\r|\01");

  /**
   * This drops all default Hive delimiters from the string and passes it on.
   *
   * These delimiters are \n, \r and \01. This method is invoked when the
   * command line option {@code --hive-drop-delims} is provided.
   *
   * @param str
   * @param delimiters
   * @return
   */
  public static String hiveStringDropDelims(String str,
      com.cloudera.sqoop.lib.DelimiterSet delimiters) {
    return hiveStringReplaceDelims(str, "", delimiters);
  }

  /**
   * replace hive delimiters with a user-defined string passed to the
   * --hive-delims-replacement option.
   * @param str
   * @param delimiters
   * @return
   */
  public static String hiveStringReplaceDelims(String str, String replacement,
      com.cloudera.sqoop.lib.DelimiterSet delimiters) {
    String droppedDelims = REPLACE_PATTERN.matcher(str).replaceAll(replacement);
    return escapeAndEnclose(droppedDelims, delimiters);
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
  public static String escapeAndEnclose(String str,
      com.cloudera.sqoop.lib.DelimiterSet delimiters) {

    char escape = delimiters.getEscapedBy();
    char enclose = delimiters.getEnclosedBy();
    boolean encloseRequired = delimiters.isEncloseRequired();

    // true if we can use an escape character.
    boolean escapingLegal =
        com.cloudera.sqoop.lib.DelimiterSet.NULL_CHAR != escape;
    String withEscapes;

    if (null == str) {
      return null;
    }

    if (escapingLegal) {
      // escaping is legal. Escape any instances of the escape char itself.
      withEscapes = str.replace("" + escape, "" + escape + escape);
    } else {
      // no need to double-escape
      withEscapes = str;
    }

    if (com.cloudera.sqoop.lib.DelimiterSet.NULL_CHAR == enclose) {
      // The enclose-with character was left unset, so we can't enclose items.

      if (escapingLegal) {
        // If the user has used the fields-terminated-by or
        // lines-terminated-by characters in the string, escape them if we
        // have an escape character.
        String fields = "" + delimiters.getFieldsTerminatedBy();
        String lines = "" + delimiters.getLinesTerminatedBy();
        withEscapes = withEscapes.replace(fields, "" + escape + fields);
        withEscapes = withEscapes.replace(lines, "" + escape + lines);
      }

      // No enclosing possible, so now return this.
      return withEscapes;
    }

    // if we have an enclosing character, and escaping is legal, then the
    // encloser must always be escaped.
    if (escapingLegal) {
      withEscapes = withEscapes.replace("" + enclose, "" + escape + enclose);
    }

    boolean actuallyDoEnclose = encloseRequired;
    if (!actuallyDoEnclose) {
      // check if the string requires enclosing.
      char [] mustEncloseFor = new char[2];
      mustEncloseFor[0] = delimiters.getFieldsTerminatedBy();
      mustEncloseFor[1] = delimiters.getLinesTerminatedBy();
      for (char reason : mustEncloseFor) {
        if (str.indexOf(reason) != -1) {
          actuallyDoEnclose = true;
          break;
        }
      }
    }

    if (actuallyDoEnclose) {
      return "" + enclose + withEscapes + enclose;
    } else {
      return withEscapes;
    }
  }

  private FieldFormatter() { }
}
