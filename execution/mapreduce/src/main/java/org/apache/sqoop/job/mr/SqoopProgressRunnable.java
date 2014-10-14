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
package org.apache.sqoop.job.mr;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;


/**
  * Runnable that will ping mapreduce context about progress.
  */
public class SqoopProgressRunnable implements Runnable {

  public static final Logger LOG = Logger.getLogger(SqoopProgressRunnable.class);

  /**
   * Context class that we should use for reporting progress.
   */
  private final TaskInputOutputContext<?,?,?,?> context;

  public SqoopProgressRunnable(final TaskInputOutputContext<?,?,?,?> ctx) {
    this.context = ctx;
  }

  @Override
  public void run() {
    LOG.debug("Auto-progress thread reporting progress");
    this.context.progress();
  }
}
