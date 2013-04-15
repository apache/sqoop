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
package org.apache.sqoop.client.shell;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.core.Constants;
import org.codehaus.groovy.tools.shell.IO;

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

  private static String serverHost = getEnv(Constants.ENV_HOST, "localhost");
  private static String serverPort = getEnv(Constants.ENV_PORT, "12000");
  private static String serverWebapp = getEnv(Constants.ENV_WEBAPP, "sqoop");

  private static boolean verbose = false;
  private static boolean interactive = false;

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

  public static String getServerUrl() {
    return "http://" + serverHost + ":" + serverPort + "/" + serverWebapp + "/";
  }

  public static ResourceBundle getResourceBundle() {
    return resource;
  }

  public static void setVerbose(boolean newValue) {
    verbose = newValue;
  }

  public static boolean isVerboose() {
    return verbose;
  }

  public static void setInteractive(boolean newValue) {
    interactive = newValue;
  }

  public static boolean isInteractive() {
    return interactive;
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
