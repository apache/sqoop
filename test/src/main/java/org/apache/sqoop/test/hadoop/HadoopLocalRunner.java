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
package org.apache.sqoop.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.test.utils.HdfsUtils;

/**
 * Represents a local cluster. It uses an unchanged Configuration object.
 * HadoopRunner implementation that is using LocalJobRunner for executing
 * mapreduce jobs and local filesystem instead of HDFS.
 */
public class HadoopLocalRunner extends HadoopRunner {

  @Override
  public Configuration prepareConfiguration(Configuration conf)
      throws Exception {
    return conf;
  }

  @Override
  public void start() throws Exception {
    // Do nothing!
  }

  @Override
  public void stop() throws Exception {
    // Do nothing!
  }

  @Override
  public String getTestDirectory() {
    return HdfsUtils.joinPathFragments(getTemporaryPath(), "/mapreduce-job-io");
  }

}
