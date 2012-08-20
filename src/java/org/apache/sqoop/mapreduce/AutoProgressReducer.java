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

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Identity reducer that continuously reports progress via a background thread.
 */
public class AutoProgressReducer<KEYIN, VALIN, KEYOUT, VALOUT>
    extends SqoopReducer<KEYIN, VALIN, KEYOUT, VALOUT> {

  public static final Log LOG = LogFactory.getLog(
      AutoProgressReducer.class.getName());

  // reduce() method intentionally omitted;
  // Reducer.reduce() is the identity reducer.

  /**
   * Run the mapping process for this task, wrapped in an auto-progress system.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    ProgressThread thread = new ProgressThread(context, LOG);

    try {
      thread.setDaemon(true);
      thread.start();

      // use default run() method to actually drive the mapping.
      super.run(context);
    } finally {
      // Tell the progress thread to exit..
      LOG.debug("Instructing auto-progress thread to quit.");
      thread.signalShutdown();
      try {
        // And wait for that to happen.
        LOG.debug("Waiting for progress thread shutdown...");
        thread.join();
        LOG.debug("Progress thread shutdown detected.");
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted when waiting on auto-progress thread: "
            + ie.toString(), ie);
      }
    }
  }
}
