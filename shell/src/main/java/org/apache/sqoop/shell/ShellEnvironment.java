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
package org.apache.sqoop.shell;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.core.Constants;
import org.codehaus.groovy.tools.shell.IO;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Static internal environment of the shell shared across all commands and
 * functions.
 */
public final class ShellEnvironment {
  private ShellEnvironment() {
    // Direct instantiation is prohibited as entire functionality is exposed
    // using static API.
  }

  private static final long DEFAULT_POLL_TIMEOUT = 10000;

  private static String DEFAULT_SERVER_HOST = getEnv(Constants.ENV_HOST, "localhost");
  private static String DEFAULT_SERVER_PORT = getEnv(Constants.ENV_PORT, "12000");
  private static String DEFAULT_SERVER_WEBAPP = getEnv(Constants.ENV_WEBAPP, "sqoop");

  private static String serverHost = DEFAULT_SERVER_HOST;
  private static String serverPort = DEFAULT_SERVER_PORT;
  private static String serverWebapp = DEFAULT_SERVER_WEBAPP;

  private static boolean verbose = false;
  private static boolean interactive = false;
  private static long pollTimeout = DEFAULT_POLL_TIMEOUT;

  static ResourceBundle resource = ResourceBundle.getBundle(Constants.RESOURCE_NAME, Locale.getDefault());
  static SqoopClient client = new SqoopClient(getServerUrl());
  static IO io;

  public static String getEnv(String variable, String defaultValue) {
    String value = System.getenv(variable);
    return value != null ? value : defaultValue;
  }

  public static SqoopClient getClient() {
    return client;
  }

  public static void setIo(IO ioObject) {
    io = ioObject;
  }

  public static IO getIo() {
    return io;
  }

  public static void setServerHost(String host) {
    serverHost = host;
    client.setServerUrl(getServerUrl());
  }

  public static String getServerHost() {
    return serverHost;
  }

  public static void setServerPort(String port) {
    serverPort = port;
    client.setServerUrl(getServerUrl());
  }

  public static String getServerPort() {
    return serverPort;
  }

  public static void setServerWebapp(String webapp) {
    serverWebapp = webapp;
    client.setServerUrl(getServerUrl());
  }

  public static String getServerWebapp() {
    return serverWebapp;
  }

  public static void setServerUrl(String ustr){
    try {
      URL url = new URL(ustr);

      String host = url.getHost();
      if (host.length() > 0) {
        serverHost = host;
      }

      int port = url.getPort();
      if (port != -1) {
        serverPort = Integer.toString(port);
      } else {
        // use default port number
        serverPort = DEFAULT_SERVER_PORT;
      }

      String webapp = url.getFile();
      if (webapp.length() > 1) {
        // get rid of the first slash
        serverWebapp = webapp.substring(1);
      } else {
        // use default webapp name
        serverWebapp = DEFAULT_SERVER_WEBAPP;
      }

      client.setServerUrl(getServerUrl());
    } catch (MalformedURLException ex) {
      throw new SqoopException(ShellError.SHELL_0003, ex);
    }
  }

  public static String getServerUrl() {
    return "http://" + serverHost + ":" + serverPort + "/" + serverWebapp + "/";
  }

  public static ResourceBundle getResourceBundle() {
    return resource;
  }

  public static void setVerbose(boolean newValue) {
    verbose = newValue;
  }

  public static boolean isVerbose() {
    return verbose;
  }

  public static void setInteractive(boolean newValue) {
    interactive = newValue;
  }

  public static boolean isInteractive() {
    return interactive;
  }

  public static void setPollTimeout(long timeout) {
    pollTimeout = timeout;
  }

  public static long getPollTimeout() {
    return pollTimeout;
  }

  public static String resourceString(String resourceName) {
    return resource.getString(resourceName);
  }

  public static void printlnResource(String resourceName) {
    io.out.println(resource.getString(resourceName));
  }

  public static void printlnResource(String resourceName, Object... values) {
    io.out.println(MessageFormat.format(resourceString(resourceName), values));
  }

  public static void println(String str, Object ... values) {
    io.out.println(MessageFormat.format(str, values));
  }

  public static void println(String str) {
    io.out.println(str);
  }

  public static void println(Object obj) {
    io.out.println(obj);
  }

  public static void println() {
    io.out.println();
  }

  public static void print(String str) {
    io.out.print(str);
  }

  public static void print(Object obj) {
    io.out.print(obj);
  }

  public static void print(String format, Object... args) {
    io.out.printf(format, args);
  }
}

