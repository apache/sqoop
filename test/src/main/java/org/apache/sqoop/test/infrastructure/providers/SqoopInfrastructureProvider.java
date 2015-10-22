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
package org.apache.sqoop.test.infrastructure.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniClusterFactory;

/**
 * Sqoop infrastructure provider.
 */
public class SqoopInfrastructureProvider extends InfrastructureProvider {
  private static final Logger LOG = Logger.getLogger(SqoopInfrastructureProvider.class);

  private SqoopMiniCluster instance;
  private String rootPath;
  private Configuration hadoopConf;

  public SqoopInfrastructureProvider() {}

  @Override
  public void start() {
    try {
      instance = SqoopMiniClusterFactory.getSqoopMiniCluster(System.getProperties(), JettySqoopMiniCluster.class, rootPath, hadoopConf);
      instance.start();
    } catch (Exception e) {
      LOG.error("Could not start Sqoop mini cluster.", e);
    }
  }

  @Override
  public void stop() {
    try {
      instance.stop();
    } catch (Exception e) {
      LOG.error("Could not stop Sqoop mini cluster.", e);
    }
  }

  @Override
  public void setHadoopConfiguration(Configuration conf) {
    hadoopConf = conf;
  }

  @Override
  public Configuration getHadoopConfiguration() {
    return hadoopConf;
  }

  @Override
  public void setRootPath(String path) {
    rootPath = path;
  }

  @Override
  public String getRootPath() {
    return rootPath;
  }

  public SqoopMiniCluster getInstance() {
    return instance;
  }
}
