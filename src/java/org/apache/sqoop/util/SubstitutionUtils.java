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
package org.apache.sqoop.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for null substitution character handling.
 *
 */
public final class SubstitutionUtils {

  // List of items that needs to be de-escaped in order to be consistent with
  // Sqoop interpretation of the NULL string parameters.
  private static final Map<String, String> REMOVE_ESCAPE_CHARS;

  static {
    // Build static map of escape characters that needs to be de-escaped.
    // http://docs.oracle.com/javase/specs/jls/se7/html/jls-3.html#jls-3.10.6
    REMOVE_ESCAPE_CHARS = new HashMap<String, String>();
    REMOVE_ESCAPE_CHARS.put("\\\\b", "\b");
    REMOVE_ESCAPE_CHARS.put("\\\\t", "\t");
    REMOVE_ESCAPE_CHARS.put("\\\\n", "\n");
    REMOVE_ESCAPE_CHARS.put("\\\\f", "\f");
    REMOVE_ESCAPE_CHARS.put("\\\\'", "'");
    REMOVE_ESCAPE_CHARS.put("\\\\\"", "\"");
    REMOVE_ESCAPE_CHARS.put("\\\\\\\\", "\\\\");
    // TODO(jarcec, optional): Deal with octal escape sequences?
  }

  /**
   * De-escape all escape sequences presented in the string.
   *
   * Sqoop is historically using --(input)-null-(non-)string parameters directly
   * in generated code and thus they need to be manually escaped on command
   * line. However some connectors might need their final form, so that they
   * can be also used outside generated code.
   *
   * @param string String to de-escape
   * @return String without escape sequences
   */
  public static String removeEscapeCharacters(String string) {

    // Our de-escaping is not suporting octal escape sequences
    if (string.matches("\\\\[0-9]+")) {
      throw new RuntimeException("Octal escape sequence is not supported");
    }

    for (Map.Entry<String, String> entry : REMOVE_ESCAPE_CHARS.entrySet()) {
      string = string.replaceAll(entry.getKey(), entry.getValue());
    }
    return string;
  }

  private SubstitutionUtils() {
    // This class can't be instantiated
  }
}
