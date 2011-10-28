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

/**
 * Parse string representations of boolean values into boolean
 * scalar types.
 */
public final class BooleanParser {

  /**
   * Return a boolean based on the value contained in the string.
   *
   * <p>The following values are considered true:
   * "true", "t", "yes", "on", "1".</p>
   * <p>All other values, including 'null', are false.</p>
   * <p>All comparisons are case-insensitive.</p>
   */
  public static boolean valueOf(final String s) {
    return s != null && ("true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s)
        || "1".equals(s) || "on".equalsIgnoreCase(s)
        || "yes".equalsIgnoreCase(s));
  }

  private BooleanParser() { }
}
