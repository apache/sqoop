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

package com.cloudera.sqoop.mapreduce;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class ExportInputFormat
   extends org.apache.sqoop.mapreduce.ExportInputFormat {

  public static final int DEFAULT_NUM_MAP_TASKS =
      org.apache.sqoop.mapreduce.ExportInputFormat.DEFAULT_NUM_MAP_TASKS;

  public static void setNumMapTasks(JobContext job, int numTasks) {
    org.apache.sqoop.mapreduce.ExportInputFormat.setNumMapTasks(job, numTasks);
  }

  public static int getNumMapTasks(JobContext job) {
    return org.apache.sqoop.mapreduce.ExportInputFormat.getNumMapTasks(job);
  }

}
