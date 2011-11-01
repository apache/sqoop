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

/**
 * Describes a SqoopTool.
 * This class should be final
 */
public class ToolDesc {
  private final String toolName;
  private final Class<? extends SqoopTool> toolClass;
  private final String description;


  /**
   * Main c'tor; sets all fields that describe a SqoopTool.
   */
  public ToolDesc(String name, Class<? extends SqoopTool> cls, String desc) {
    this.toolName = name;
    this.toolClass = cls;
    this.description = desc;
  }

  /**
   * @return the name used to invoke the tool (e.g., 'sqoop &lt;foo&gt;')
   */
  public String getName() {
    return toolName;
  }

  /**
   * @return a human-readable description of what the tool does.
   */
  public String getDesc() {
    return description;
  }

  /**
   * @return the class that implements SqoopTool.
   */
  public Class<? extends SqoopTool> getToolClass() {
    return toolClass;
  }

}
