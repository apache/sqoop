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

package org.apache.sqoop.manager.oracle.util;

import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.sql.RowId;

/**
 * Generates ROWID test data. ROWIDs are represented by 18 ASCII encoded
 * characters from the set A-Za-z0-9/+
 *
 * Generated ROWIDs are unlikely to represent actual rows in any Oracle
 * database, so should be used for import/export tests only, and not used to
 * reference data.
 */
public class RowIdGenerator extends OraOopTestDataGenerator<RowId> {
  private static final String VALID_CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+";
  private static final int LENGTH = 18;
  private static Class<?> rowIdClass;
  private static Constructor<?> rowIdConstructor;

  static {
    try {
      rowIdClass = Class.forName("oracle.sql.ROWID");
      rowIdConstructor = rowIdClass.getConstructor(byte[].class);
    } catch (Exception e) {
      throw new RuntimeException(
          "Problem getting Oracle JDBC methods via reflection.", e);
    }
  }

  @Override
  public RowId next() {
    try {
      StringBuffer sb = new StringBuffer();
      while (sb.length() < LENGTH) {
        sb.append(VALID_CHARS.charAt(rng.nextInt(VALID_CHARS.length())));
      }
      return (RowId) rowIdConstructor.newInstance(sb.toString().getBytes(
          Charset.forName("US-ASCII")));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
