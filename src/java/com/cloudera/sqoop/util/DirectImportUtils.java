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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.io.SplittableBufferedWriter;
import com.cloudera.sqoop.manager.ImportJobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class DirectImportUtils {

  private DirectImportUtils() { }

  public static void setFilePermissions(File file, String modstr)
      throws IOException {
    org.apache.sqoop.util.DirectImportUtils.setFilePermissions(file, modstr);
  }

  public static SplittableBufferedWriter createHdfsSink(Configuration conf,
      SqoopOptions options, ImportJobContext context) throws IOException {
    return org.apache.sqoop.util.DirectImportUtils.createHdfsSink(conf,
        options, context);
  }

  public static boolean isLocalhost(String someHost) {
    return org.apache.sqoop.util.DirectImportUtils.isLocalhost(someHost);
  }

}
