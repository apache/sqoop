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

package org.apache.sqoop.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.config.ConfigurationConstants;

/**
 * Utility class; returns task attempt Id of the current job
 * regardless of Hadoop version being used.
 */
public final class TaskId {

  private TaskId() { }

  /**
   * Return the task attempt id as a string.
   * @param conf the Configuration to check for the current task attempt id.
   * @param defaultVal the value to return if a task attempt id is not set.
   * @return the current task attempt id, or the default value if one isn't set.
   */
  public static String get(Configuration conf, String defaultVal) {
    return conf.get("mapreduce.task.id",
        conf.get("mapred.task.id", defaultVal));
  }

  /**
   * Return the local filesystem dir where the current task attempt can
   * perform work.
   * @return a File describing a directory where local temp data for the
   * task attempt can be stored.
   */
  public static File getLocalWorkPath(Configuration conf) throws IOException {
    String tmpDir = conf.get(
        ConfigurationConstants.PROP_JOB_LOCAL_DIRECTORY,
        "/tmp/");

    // Create a local subdir specific to this task attempt.
    String taskAttemptStr = TaskId.get(conf, "task_attempt");
    File taskAttemptDir = new File(tmpDir, taskAttemptStr);
    if (!taskAttemptDir.exists()) {
      boolean createdDir = taskAttemptDir.mkdirs();
      if (!createdDir) {
        throw new IOException("Could not create missing task attempt dir: "
            + taskAttemptDir.toString());
      }
    }

    return taskAttemptDir;
  }

}
