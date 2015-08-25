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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * Sqoop specific listener that will print name of each particular test that being run.
 *
 * Particularly suitable for integration tests that generally takes long time to finish.
 */
public class SqoopTestListener extends TestListenerAdapter {
  private static final Logger LOG = Logger.getLogger(SqoopTestListener.class);
  /**
   * Sqoop tests are generally running with redirectTestOutputToFile=true which means
   * that System.out is redirected to file. That is unpleasant as output user is suppose
   * to see is not available on the console. Hence instead of System.out we're using
   * directly the STDOUT file descriptor.
   */
  static PrintStream ps;

  static {
    try {
      ps = new PrintStream(new FileOutputStream(FileDescriptor.out), false, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("Error when initialize the SqoopTestListener.", e);
    }
  }


  @Override
  public void onTestStart(ITestResult tr) {
    ps.flush();
    ps.print(testName(tr));
    ps.print(" running...\n");
  }

  @Override
  public void onTestFailure(ITestResult tr) {
    ps.flush();
    ps.print("FAILURE");
    ps.print(elapsedTime(tr));
    ps.print("\n\n");
  }

  @Override
  public void onTestSkipped(ITestResult tr) {
    ps.flush();
    ps.print("SKIPPED");
    ps.print(elapsedTime(tr));
    ps.print("\n\n");
  }

  @Override
  public void onTestSuccess(ITestResult tr) {
    ps.flush();
    ps.print("SUCCESS");
    ps.print(elapsedTime(tr));
    ps.print("\n\n");
  }

  private String testName(ITestResult tr) {
    StringBuilder sb = new StringBuilder("Test ")
      .append(tr.getTestClass().getName())
      .append(".")
      .append(tr.getName());

    if(tr.getParameters() != null && tr.getParameters().length > 0) {
      sb.append(" with parameters [")
        .append(StringUtils.join(tr.getParameters(), ","))
        .append("]");
    }

    return sb.toString();
  }

  private String elapsedTime(ITestResult tr) {
    StringBuilder sb = new StringBuilder(" in ")
      .append((tr.getEndMillis() - tr.getStartMillis()) / 1000)
      .append(" seconds");

    return sb.toString();
  }
}
