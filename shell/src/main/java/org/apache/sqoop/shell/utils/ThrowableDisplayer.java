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
package org.apache.sqoop.shell.utils;

import groovy.lang.MissingPropertyException;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.ShellError;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Pretty printing of Throwable objects
 */
public class ThrowableDisplayer {

  /**
   * Error hook installed to Groovy shell.
   *
   * Will display exception that appeared during executing command. In most
   * cases we will simply delegate the call to printing throwable method,
   * however in case that we've received ClientError.CLIENT_0006 (server
   * exception), we will unwrap server issue and view only that as local
   * context shouldn't make any difference.
   *
   * @param t Throwable to be displayed
   */
  public static void errorHook(Throwable t) {
    // Based on the kind of exception we are dealing with, let's provide different user experince
    if(t instanceof SqoopException && ((SqoopException)t).getErrorCode() == ShellError.SHELL_0006) {
      println("@|red Server has returned exception: |@");
      printThrowable(t.getCause(), isVerbose());
    } else if(t instanceof SqoopException && ((SqoopException)t).getErrorCode() == ShellError.SHELL_0003) {
      print("@|red Invalid command invocation: |@");
      // In most cases the cause will be actual parsing error, so let's print that alone
      if (t.getCause() != null) {
        println(t.getCause().getMessage());
      } else {
        println(t.getMessage());
      }
    } else if(t.getClass() == MissingPropertyException.class) {
      print("@|red Unknown command: |@");
      println(t.getMessage());
    } else {
      println("@|red Exception has occurred during processing command |@");
      printThrowable(t, isVerbose());
    }
  }

  /**
   * Pretty print Throwable instance including stack trace and causes.
   *
   * @param t Throwable to display
   */
  protected static void printThrowable(Throwable t, boolean verbose) {
    print("@|red Exception: |@");
    print(t.getClass().getName());
    print(" @|red Message: |@");
    print(t.getMessage());
    println();

    if(verbose) {
      println("Stack trace:");
      for(StackTraceElement e : t.getStackTrace()) {
        print("\t @|bold at |@ ");
        print(e.getClassName());
        print(" (@|bold " + e.getFileName() + ":" + e.getLineNumber() + ") |@ ");
        println();
      }

      Throwable cause = t.getCause();
      if(cause != null) {
        print("Caused by: ");
        printThrowable(cause, verbose);
      }
    }
  }

  private ThrowableDisplayer() {
    // Instantiation is prohibited
  }
}
