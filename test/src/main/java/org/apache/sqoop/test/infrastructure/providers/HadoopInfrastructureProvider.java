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
import org.apache.sqoop.test.hadoop.HadoopMiniClusterRunner;
import org.apache.sqoop.test.hadoop.HadoopRunner;
import org.apache.sqoop.test.hadoop.HadoopRunnerFactory;
import org.apache.sqoop.test.kdc.KdcRunner;

/**
 * Hadoop infrastructure provider.
 */
public class HadoopInfrastructureProvider extends InfrastructureProvider {
  private static final Logger LOG = Logger.getLogger(HadoopInfrastructureProvider.class);

  private HadoopRunner instance;

  public HadoopInfrastructureProvider() {
    try {
      instance = HadoopRunnerFactory.getHadoopCluster(System.getProperties(), HadoopMiniClusterRunner.class);
    } catch (Exception e) {
      LOG.error("Error fetching Hadoop runner.", e);
    }
  }

  @Override
  public void start() {
    try {
      instance.start();
    } catch(Exception e) {
      LOG.error("Could not start hadoop runner.", e);
    }
  }

  @Override
  public void stop() {
    try {
      instance.stop();
    } catch(Exception e) {
      LOG.error("Could not stop hadoop runner.", e);
    }
  }

  @Override
  public void setHadoopConfiguration(Configuration conf) {
    try {
      instance.setConfiguration(instance.prepareConfiguration(conf));
    } catch (Exception e) {
      LOG.error("Could not set configuration.", e);
    }
  }

  @Override
  public Configuration getHadoopConfiguration() {
    return instance.getConfiguration();
  }

  @Override
  public void setRootPath(String path) {
    instance.setTemporaryPath(path);
  }

  @Override
  public String getRootPath() {
    return instance.getTemporaryPath();
  }

  public HadoopRunner getInstance() {
    return instance;
  }

  @Override
  public void setKdc(KdcRunner kdc) {
    // Do nothing for the time being. Need to handle this when we support kerberos enabled MiniCluster.
  }
}
