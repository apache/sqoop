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

package com.cloudera.sqoop.util;

import java.io.IOException;
import java.util.List;

import org.apache.sqoop.util.AsyncSink;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public final class Executor {

  private Executor() { }

  public static int exec(String [] args) throws IOException {
    return org.apache.sqoop.util.Executor.exec(args);
  }

  public static int exec(String [] args, AsyncSink outSink,
      AsyncSink errSink) throws IOException {
    return org.apache.sqoop.util.Executor.exec(args, outSink, errSink);
  }

  public static int exec(String [] args, String [] envp, AsyncSink outSink,
      AsyncSink errSink) throws IOException {
    return org.apache.sqoop.util.Executor.exec(args, envp, outSink, errSink);
  }

  public static List<String> getCurEnvpStrings() {
    return org.apache.sqoop.util.Executor.getCurEnvpStrings();
  }

}
