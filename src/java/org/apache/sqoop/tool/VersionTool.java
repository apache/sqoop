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

package org.apache.sqoop.tool;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.cli.ToolOptions;

/**
 * Tool that prints Sqoop's version.
 */
public class VersionTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public VersionTool() {
    super("version");
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    System.out.print(new org.apache.sqoop.SqoopVersion().toString());
    return 0;
  }

  @Override
  public void printHelp(ToolOptions opts) {
    System.out.println("usage: sqoop " + getToolName());
  }
}

