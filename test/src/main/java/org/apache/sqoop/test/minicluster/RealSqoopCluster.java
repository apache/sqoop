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
package org.apache.sqoop.test.minicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

/**
 */
public class RealSqoopCluster extends SqoopMiniCluster {

  private final static String SERVER_URL_KEY = "org.apache.sqoop.minicluster.real.server_url";

  private String serverUrl;

  private SqoopClient client;

  public RealSqoopCluster(String temporaryPath) throws Exception {
    super(temporaryPath);

    serverUrl = System.getProperty(SERVER_URL_KEY);

    client = new SqoopClient(serverUrl);

    if(serverUrl == null) {
      throw new RuntimeException("Missing URL for real Sqoop 2 server: " + SERVER_URL_KEY);
    }
  }

  public RealSqoopCluster(String temporaryPath, Configuration configuration) throws Exception {
    this(temporaryPath);
    // We're ignoring Hadoop configuration as we're running against real cluster
  }

  @Override
  public void start() throws Exception {
    client.deleteAllLinksAndJobs();
  }

  @Override
  public void stop() throws Exception {
    client.deleteAllLinksAndJobs();
  }

  @Override
  public String getServerUrl() {
    return serverUrl;
  }

  @Override
  public String getConfigurationPath() {
    return "/etc/hadoop/conf/";
  }
}
