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
package org.apache.sqoop.tools;

/**
 * Abstract class describing Sqoop tool.
 *
 * Tools are server side actions that administrator might need to execute as
 * one time action. They do not serve as a service and are not participating
 * in data transfers (unlike Sqoop 1 tools). They are strictly maintenance
 * related such as "validate deployment" or "upgrade repository".
 *
 * All tools are executed in the same environment as Sqoop server itself (classpath,
 * configuration, java properties, ...).
 */
public abstract class Tool {

  /**
   * Run the tool.
   *
   * @param arguments Arguments as were entered on the command line
   * @return true if the execution was successful
   */
  abstract public boolean runTool(String[] arguments);
}
