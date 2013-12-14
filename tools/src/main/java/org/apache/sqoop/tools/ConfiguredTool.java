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
package org.apache.sqoop.tools;

import org.apache.sqoop.core.SqoopConfiguration;

/**
 * Most of the tools needs to load Sqoop configuration in order to perform
 * their task. To simplify the code, ConfiguredTool will make sure that
 * the configuration is properly loaded prior executing the actual tool.
 */
public abstract class ConfiguredTool extends Tool {

  public abstract boolean runToolWithConfiguration(String[] arguments);

  @Override
  public final boolean runTool(String[] arguments) {
    try {
      SqoopConfiguration.getInstance().initialize();
      return runToolWithConfiguration(arguments);
    } finally {
      SqoopConfiguration.getInstance().destroy();
    }
  }

}
