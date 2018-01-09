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

package org.apache.sqoop.orm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.sqoop.SqoopOptions;

/**
 * Reconciles the table name being imported with the class naming information
 * specified in SqoopOptions to determine the actual package and class name to
 * use for a table.
 */
public class TableClassName {

  public static final Log LOG = LogFactory.getLog(
      TableClassName.class.getName());

  public static final String QUERY_RESULT = "QueryResult";

  private final SqoopOptions options;

  public TableClassName(final SqoopOptions opts) {
    if (null == opts) {
      throw new NullPointerException(
          "Cannot instantiate a TableClassName on null options.");
    } else {
      this.options = opts;
    }
  }

  /**
   * Taking into account --class-name and --package-name, return the actual
   * package-part which will be used for a class. The actual table name being
   * generated-for is irrelevant; so not an argument.
   *
   * @return the package where generated ORM classes go. Will be null for
   * top-level.
   */
  public String getPackageForTable() {
    String predefinedClass = options.getClassName();
    if (null != predefinedClass) {
      // If the predefined classname contains a package-part, return that.
      int lastDot = predefinedClass.lastIndexOf('.');
      if (-1 == lastDot) {
        // No package part.
        return null;
      } else {
        // Return the string up to but not including the last dot.
        return predefinedClass.substring(0, lastDot);
      }
    } else {
      // If the user has specified a package name, return it.
      // This will be null if the user hasn't specified one -- as we expect.
      return options.getPackageName();
    }
  }

  /**
   * @param tableName the name of the table being imported.
   * @return the full name of the class to generate/use to import a table.
   */
  public String getClassForTable(String tableName) {
    String predefinedClass = options.getClassName();
    if (predefinedClass != null) {
      // The user's chosen a specific class name for this job.
      return predefinedClass;
    }

    String queryName = tableName;
    if (null == queryName) {
      queryName = QUERY_RESULT;
    }

    String packageName = options.getPackageName();
    if (null != packageName) {
      // return packageName.queryName.
      return packageName + "." + queryName;
    }

    // no specific class; no specific package.
    // Just make sure it's a legal identifier.
    return ClassWriter.toJavaIdentifier(queryName);
  }

  /**
   * @return just the last segment of the class name -- all package info
   * stripped.
   */
  public String getShortClassForTable(String tableName) {
    String fullClass = getClassForTable(tableName);
    if (null == fullClass) {
      return null;
    }

    int lastDot = fullClass.lastIndexOf('.');
    if (-1 == lastDot) {
      return fullClass;
    } else {
      return fullClass.substring(lastDot + 1, fullClass.length());
    }
  }
}
