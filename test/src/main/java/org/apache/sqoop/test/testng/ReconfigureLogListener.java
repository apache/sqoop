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
package org.apache.sqoop.test.testng;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * Sqoop is running as much tests as possible inside one suite to safe time starting
 * miniclusters which is time consuming exercise (~40 seconds per single test class).
 * That however means that we have one output log file that recently grown to more then
 * 1GB in side and hence the usability has decreased.
 *
 * This listener will intercept each test and will reconfigure log4j to log directly
 * into files rather then to console (that would be forwarded by maven surefire plugin to
 * the normal log file). Each test will get it's own file which is easier for human to
 * read.
 *
 * We're using a counter to order log files per execution order rather then per name as
 * we can't guarantee log isolation entirely (e.g. some information relevant to test N
 * can be in log file for test N-1). It's easier to open previous log if you immediately
 * know what is the previous log.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
public class ReconfigureLogListener  extends TestListenerAdapter {

  /**
   * Directory into which we should put all log files.
   */
  private static final String TEST_LOG_DIRECTORY = System.getProperty("sqoop.integration.log");

  /**
   * We're purposely configuring some of the loggers into a different level then DEBUG
   * as they are more spamming the log then being actually helpful. As with all logs,
   * it can happen that they might be needed at some point, so rather then hardcoding
   * the rules in code, we've chosen path to expose ability to override them on the
   * command line.
   */
  private static final String TEST_LOGGERS = System.getProperty("sqoop.integration.log.loggers",
    "org.eclipse.jetty=INFO," +
    "org.apache.directory=INFO," +
    "org.apache.hadoop.ipc.Server=INFO," +
    "org.apache.hadoop.hdfs=INFO," +
    "org.apache.hadoop.security.SaslInputStream=INFO," +
    "org.apache.hadoop.security.SaslRpcClient=INFO," +
    "org.apache.hadoop.ipc.Client=INFO," +
    "org.apache.hadoop.conf.Configuration=INFO"
  );

  // Parsed and cached variant of TEST_LOGGERS
  private static Map<String, Level> loggerConfiguration;
  static {
    loggerConfiguration = new HashMap<>();
    for(String rule : TEST_LOGGERS.split(",")) {
      String []split = rule.split("=");
      if(split.length != 2) {
        throw new RuntimeException("Incorrect rule, expected logger=level: " + rule);
      }

      loggerConfiguration.put(split[0].trim(), Level.toLevel(split[1].trim()));
    }
  }

  /**
   * Counter is increased for each test execution
   */
  private static int counter = 0;

  /**
   * On every test start, we'll start logging into different file
   */
  @Override
  public void onTestStart(ITestResult tr) {
    // Reset log4j configuration on every test run
    Logger.getRootLogger().getLoggerRepository().resetConfiguration();

    // Get test # so that our resulting files are sorted by execution and not by name
    int ourCounter = counter++;

    // Usual File appender
    FileAppender appender = new FileAppender();
    appender.setName("Sqoop test dynamic logger");
    appender.setFile(TEST_LOG_DIRECTORY + "/" + String.format("%05d", ourCounter) + "_" + tr.getTestClass().getName() + "." + tr.getName() + ".txt");
    appender.setLayout(new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n"));
    appender.setImmediateFlush(true);
    appender.setThreshold(Level.DEBUG);
    appender.activateOptions();

    // Different levels for various not-so important loggers
    Logger.getRootLogger().addAppender(appender);
    for(Map.Entry<String, Level> entry : loggerConfiguration.entrySet()) {
      Logger.getLogger(entry.getKey()).setLevel(entry.getValue());
    }
  }

}
