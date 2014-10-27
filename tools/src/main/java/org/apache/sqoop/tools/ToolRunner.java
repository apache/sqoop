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

import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.tools.tool.BuiltinTools;
import org.apache.sqoop.utils.ClassUtils;

import java.util.Arrays;

/**
 * Runner allowing the tool to be executed from command line.
 *
 * It's expected that all usual dependencies are already on the classpath.
 */
public final class ToolRunner {

  /**
   * Run tool given as first argument and pass all other arguments to it as
   * tool parameters.
   *
   * @param args Tool name and it's arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.out.println("Sqoop tool executor:");
    System.out.println("\tVersion: " + VersionInfo.getBuildVersion());
    System.out.println("\tRevision: " + VersionInfo.getSourceRevision());
    System.out.println("\tCompiled on " + VersionInfo.getBuildDate() + " by " + VersionInfo.getUser());

    if(args.length < 1) {
      throw new IllegalArgumentException("Name of the tool is missing.");
    }

    Class<? extends Tool> toolClass;

    // Firstly let's try to resolve short name
    toolClass = BuiltinTools.getTool(args[0]);

    // If the short name wasn't resolved, find the class by it's name
    if(toolClass == null) {
      toolClass = (Class<? extends Tool>) ClassUtils.loadClass(args[0]);
    }

    if(toolClass == null) {
      throw new IllegalArgumentException("Can't find tool: " + args[0]);
    }

    Tool tool = (Tool) ClassUtils.instantiate(toolClass);
    if(tool == null) {
      throw new RuntimeException("Can't get tool instance: " + args[0]);
    }

    // We do want us to kill the Tomcat when running from tooling
    System.setProperty(ConfigurationConstants.KILL_TOMCAT_ON_FAILURE, "false");

    System.out.println("Running tool: " + toolClass);
    if(tool.runTool(Arrays.copyOfRange(args, 1, args.length))) {
      System.out.println("Tool " + toolClass + " has finished correctly.");
    } else {
      System.out.println("Tool " + toolClass + " has failed.");
      System.exit(1);
    }
  }

  private ToolRunner() {
    // Instantiation is prohibited
  }
}
