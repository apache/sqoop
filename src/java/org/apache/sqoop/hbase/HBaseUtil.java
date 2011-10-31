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

package org.apache.sqoop.hbase;

/**
 * This class provides a method that checks if HBase jars are present in the
 * current classpath. It also provides a setAlwaysNoHBaseJarMode mechanism for
 * testing and simulation the condition where the is on HBase jar (since hbase
 * is pulled automatically by ivy)
 */
public final class HBaseUtil {

  private static boolean testingMode = false;

  private HBaseUtil() {
  }

  /**
   * This is a way to make this always return false for testing.
   */
  public static void setAlwaysNoHBaseJarMode(boolean mode) {
    testingMode = mode;
  }

  public static boolean isHBaseJarPresent() {
    if (testingMode) {
      return false;
    }
    try {
      Class.forName("org.apache.hadoop.hbase.client.HTable");
    } catch (ClassNotFoundException cnfe) {
      return false;
    }
    return true;
  }

}
