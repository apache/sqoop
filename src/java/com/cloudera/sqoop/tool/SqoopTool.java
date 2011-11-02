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
package com.cloudera.sqoop.tool;

import java.util.Set;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public abstract class SqoopTool
    extends org.apache.sqoop.tool.SqoopTool {

  public static final String TOOL_PLUGINS_KEY =
    org.apache.sqoop.tool.SqoopTool.TOOL_PLUGINS_KEY;

  public static final Set<String> getToolNames() {
    return org.apache.sqoop.tool.SqoopTool.getToolNames();
  }

  public static final SqoopTool getTool(String toolName) {
    return (SqoopTool)org.apache.sqoop.tool.SqoopTool.getTool(toolName);
  }

  public static final String getToolDescription(String toolName) {
    return org.apache.sqoop.tool.SqoopTool.getToolDescription(toolName);
  }

  public SqoopTool() {
     super();
  }

  public SqoopTool(String name) {
    super(name);
  }

}
