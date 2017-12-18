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

package com.cloudera.sqoop.util;

import org.apache.sqoop.manager.ConnManager;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class Jars {

  private Jars() { }

  public static String getSqoopJarPath() {
    return org.apache.sqoop.util.Jars.getSqoopJarPath();
  }

  public static String getJarPathForClass(Class<? extends Object> classObj) {
    return org.apache.sqoop.util.Jars.getJarPathForClass(classObj);
  }

  public static String getDriverClassJar(ConnManager mgr) {
    return org.apache.sqoop.util.Jars.getDriverClassJar(mgr);
  }

}

