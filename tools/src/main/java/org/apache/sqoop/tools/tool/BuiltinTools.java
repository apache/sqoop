/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.tools.tool;

import org.apache.sqoop.tools.Tool;

import java.util.HashMap;
import java.util.Map;

/**
 * List of built-in tools with short names for easier manipulation.
 */
public class BuiltinTools {

  /**
   * List of built-in tools
   */
  private static Map<String, Class<? extends Tool>> tools;
  static {
    tools = new HashMap<String, Class<? extends Tool>>();
    tools.put("upgrade", UpgradeTool.class);
    tools.put("verify", VerifyTool.class);
    tools.put("repositorydump", RepositoryDumpTool.class);
    tools.put("repositoryload", RepositoryLoadTool.class);
  }

  /**
   * Returns the tool class associated with the short name or NULL in case that
   * the short name is not known.
   *
   * @param name Short name for the built-in tool
   * @return
   */
  public static Class<? extends Tool> getTool(String name) {
    return tools.get(name);
  }

  private BuiltinTools() {
    // Instantiation is forbidden
  }
}
