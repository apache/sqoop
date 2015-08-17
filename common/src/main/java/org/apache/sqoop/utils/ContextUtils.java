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
package org.apache.sqoop.utils;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Utils to be used ubiquitously for contexts.
 */
public class ContextUtils {
  /**
   * Return an array of strings for given value or null by default.
   *
   * @param value Non-empty value
   * @return Array of values for given key or null in case of unknown key
   */
  public static String[] getArrayOfStrings(String value) {
    return getArrayOfStrings(value, ":");
  }

  /**
   * Return an array of strings for given value or default value.
   *
   * @param value Non-empty value
   * @param separator String separator
   * @return Array of values for given key or default value in case of unknown key
   */
  public static String[] getArrayOfStrings(String value, String separator) {
    assert !StringUtils.isBlank(value);

    return value.split(separator);
  }

  private static Set<String> getUniqueStrings(String[] strings) {
    if (strings == null) {
      return new HashSet<String>();
    }

    return new HashSet(Arrays.asList(strings));
  }

  /**
   * Return an array of unique strings for given value or null by default.
   *
   * @param value Non-empty value
   * @return Set of unique strings for given value or null in case of unknown key
   */
  public static Set<String> getUniqueStrings(String value) {
    return getUniqueStrings(getArrayOfStrings(value));
  }

  /**
   * Return an array of unique strings for given value or default value.
   *
   * @param value Non-empty value
   * @param separator String separator
   * @return Set of unique strings for given value or default value in case of unknown key
   */
  public static Set<String> getUniqueStrings(String value, String separator) {
    return getUniqueStrings(getArrayOfStrings(value, separator));
  }
}
