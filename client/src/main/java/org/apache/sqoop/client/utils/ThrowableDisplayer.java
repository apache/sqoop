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
package org.apache.sqoop.client.utils;

import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.codehaus.groovy.tools.shell.IO;

/**
 * Pretty printing of Throwable objects
 */
public class ThrowableDisplayer {

  /**
   * Associated shell IO object.
   *
   * This objects needs to be set explicitly as some of the methods are called
   * by Groovy shell without ability to pass additional arguments.
   */
  private static IO io;

  public static void setIo(IO ioObject) {
    io = ioObject;
  }

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
    io.out.println("@|red Exception has occurred during processing command |@");

    // If this is server exception from server
    if(t instanceof SqoopException
      && ((SqoopException)t).getErrorCode() == ClientError.CLIENT_0006) {
      io.out.print("@|red Server has returned exception: |@");
      printThrowable(io, t.getCause());
    } else {
      printThrowable(io, t);
    }
  }

  /**
   * Pretty print Throwable instance including stack trace and causes.
   *
   * @param io IO object to use for generating output
   * @param t Throwable to display
   */
  protected static void printThrowable(IO io, Throwable t) {
    io.out.print("@|red Exception: |@");
    io.out.print(t.getClass().getName());
    io.out.print(" @|red Message: |@");
    io.out.print(t.getMessage());
    io.out.println();

    io.out.println("Stack trace:");
    for(StackTraceElement e : t.getStackTrace()) {
      io.out.print("\t @|bold at |@ ");
      io.out.print(e.getClassName());
      io.out.print(" (@|bold " + e.getFileName() + ":"
        + e.getLineNumber() + ") |@ ");
      io.out.println();
    }

    Throwable cause = t.getCause();
    if(cause != null) {
      io.out.print("Caused by: ");
      printThrowable(io, cause);
    }
  }

  private ThrowableDisplayer() {
    // Instantiation is prohibited
  }
}
