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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.sqoop.job.io.SqoopWritable;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A reducer to perform reduce function.
 */
public class SqoopReducer extends Reducer<SqoopWritable, NullWritable, SqoopWritable, NullWritable> {

  static {
    MRConfigurationUtils.configureLogging();
  }
  public static final Logger LOG = Logger.getLogger(SqoopReducer.class);

  /**
   * Service for reporting progress to mapreduce.
   */
  private final ScheduledExecutorService progressService = Executors.newSingleThreadScheduledExecutor();

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    try {
      LOG.info("Starting progress service");
      progressService.scheduleAtFixedRate(new SqoopProgressRunnable(context), 0, 2, TimeUnit.MINUTES);

      // Delegating all functionality to our parent
      super.run(context);
    } finally {
      LOG.info("Stopping progress service");
      progressService.shutdown();
      if(!progressService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.info("Stopping progress service with shutdownNow");
        progressService.shutdownNow();
      }
    }
  }
}
