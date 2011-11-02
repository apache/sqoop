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

package com.cloudera.sqoop;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class Sqoop
    extends org.apache.sqoop.Sqoop {

  public static final Log SQOOP_LOG =
      org.apache.sqoop.Sqoop.SQOOP_LOG;

  public static final String SQOOP_RETHROW_PROPERTY =
      org.apache.sqoop.Sqoop.SQOOP_RETHROW_PROPERTY;

  public static final String SQOOP_OPTIONS_FILE_SPECIFIER =
      org.apache.sqoop.Sqoop.SQOOP_OPTIONS_FILE_SPECIFIER;

  static {
    Configuration.addDefaultResource("sqoop-site.xml");
  }

  public static int runSqoop(Sqoop sqoop, String [] args) {
    return org.apache.sqoop.Sqoop.runSqoop(sqoop, args);
  }

  public static int runTool(String [] args, Configuration conf) {
    return org.apache.sqoop.Sqoop.runTool(args, conf);
  }

  public static int runTool(String [] args) {
    return org.apache.sqoop.Sqoop.runTool(args);
  }

  public static void main(String [] args) {
    org.apache.sqoop.Sqoop.main(args);
  }

  public Sqoop(SqoopTool tool) {
    super(tool);
  }

  public Sqoop(SqoopTool tool, Configuration conf) {
    super(tool, conf);
  }

  public Sqoop(SqoopTool tool, Configuration conf, SqoopOptions opts) {
    super(tool, conf, opts);
  }

}

