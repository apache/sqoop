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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.log4j.Logger;

/**
 * Represents a minicluster setup. It creates a configuration object and mutates
 * it. Clients that need to connect to the miniclusters should use the provided
 * configuration object.
 */
public class HadoopMiniClusterRunner extends HadoopRunner {
  private static final Logger LOG = Logger
      .getLogger(HadoopMiniClusterRunner.class);

  /**
   * Hadoop HDFS cluster
   */
  protected MiniDFSCluster dfsCluster;

  /**
   * Hadoop MR cluster
   */
  protected MiniMRCluster mrCluster;

  @Override
  public Configuration prepareConfiguration(Configuration config)
      throws Exception {
    config.set("dfs.block.access.token.enable", "false");
    config.set("dfs.permissions", "true");
    config.set("hadoop.security.authentication", "simple");
    config.set("mapred.tasktracker.map.tasks.maximum", "1");
    config.set("mapred.tasktracker.reduce.tasks.maximum", "1");
    config.set("mapred.submit.replication", "1");
    config.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
    config.set("yarn.application.classpath",
        System.getProperty("java.class.path"));
    return config;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void start() throws Exception {
    System.setProperty("test.build.data", getDataDir());
    LOG.info("test.build.data set to: " + getDataDir());

    System.setProperty("hadoop.log.dir", getLogDir());
    LOG.info("log dir set to: " + getLogDir());

    // Start DFS server
    LOG.info("Starting DFS cluster...");
    dfsCluster = new MiniDFSCluster(config, 1, true, null);
    if (dfsCluster.isClusterUp()) {
      LOG.info("Started DFS cluster on port: " + dfsCluster.getNameNodePort());
    } else {
      LOG.error("Could not start DFS cluster");
    }

    // Start MR server
    LOG.info("Starting MR cluster");
    mrCluster = new MiniMRCluster(0, 0, 1, dfsCluster.getFileSystem().getUri()
        .toString(), 1, null, null, null, new JobConf(config));
    LOG.info("Started MR cluster");
    config = prepareConfiguration(mrCluster.createJobConf());
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping MR cluster");
    mrCluster.shutdown();
    LOG.info("Stopped MR cluster");

    LOG.info("Stopping DFS cluster");
    dfsCluster.shutdown();
    LOG.info("Stopped DFS cluster");
  }
}
