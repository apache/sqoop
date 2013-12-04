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
package org.apache.sqoop.tools.tool;

import org.apache.sqoop.core.SqoopServer;
import org.apache.sqoop.tools.Tool;
import org.apache.log4j.Logger;

/**
 * Try to initialize all Sqoop sub systems to verify that Sqoop 2 is correctly
 * configured and then tear it down. This tool will start and stop all subsystems
 * with the exception of servlets.
 */
public class VerifyTool extends Tool {

  public static final Logger LOG = Logger.getLogger(VerifyTool.class);

  @Override
  public boolean runTool(String[] arguments) {
    try {
      SqoopServer.initialize();
      SqoopServer.destroy();
      System.out.println("Verification was successful.");
      return true;
    } catch(Exception ex) {
      LOG.error("Got exception while initializing/destroying Sqoop server:", ex);
      System.out.println("Verification has failed, please check Server logs for further details.");
      return false;
    }
  }

}
