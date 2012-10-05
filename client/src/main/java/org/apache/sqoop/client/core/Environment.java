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
package org.apache.sqoop.client.core;

import java.util.Locale;
import java.util.ResourceBundle;

public class Environment
{
  private Environment() {
    // Disable explicit object creation
  }

  private static String serverHost;
  private static String serverPort;
  private static String serverWebapp;

  private static String HOST_DEFAULT = "localhost";
  private static String PORT_DEFAULT = "8080";
  private static String WEBAPP_DEFAULT = "sqoop";

  private static ResourceBundle resourceBundle;

  static {
    serverHost = HOST_DEFAULT;
    serverPort = PORT_DEFAULT;
    serverWebapp = WEBAPP_DEFAULT;

    resourceBundle =
      ResourceBundle.getBundle(Constants.RESOURCE_NAME, Locale.getDefault());
  }

  public static void setServerHost(String host) {
    serverHost = host;
  }

  public static String getServerHost() {
    return serverHost;
  }

  public static void setServerPort(String port) {
    serverPort = port;
  }

  public static String getServerPort() {
    return serverPort;
  }

  public static void setServerWebapp(String webapp) {
    serverWebapp = webapp;
  }

  public static String getServerWebapp() {
    return serverWebapp;
  }

  public static String getServerUrl() {
    return "http://" + serverHost + ":" + serverPort + "/" + serverWebapp + "/";
  }

  public static ResourceBundle getResourceBundle() {
    return resourceBundle;
  }
}
