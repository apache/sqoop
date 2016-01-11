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
import org.apache.sqoop.test.kdc.KdcRunner;
import org.apache.sqoop.test.kdc.KdcRunnerFactory;
import org.apache.sqoop.test.kdc.MiniKdcRunner;

/**
 * Kdc infrastructure provider.
 */
public class KdcInfrastructureProvider extends InfrastructureProvider {
  private static final Logger LOG = Logger.getLogger(KdcInfrastructureProvider.class);

  private KdcRunner instance;
  private Configuration conf;

  public KdcInfrastructureProvider() {
    try {
      instance = KdcRunnerFactory.getKdc(System.getProperties(), MiniKdcRunner.class);
    } catch (Exception e) {
      LOG.error("Error fetching Kdc runner.", e);
    }
  }

  @Override
  public void start() {
    try {
      instance.start();
    } catch (Exception e) {
      LOG.error("Could not start kdc.", e);
    }
  }

  @Override
  public void stop() {
    try {
      instance.stop();
    } catch (Exception e) {
      LOG.error("Could not stop kdc.", e);
    }
  }

  public KdcRunner getInstance() {
    return instance;
  }

  @Override
  public void setHadoopConfiguration(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getHadoopConfiguration() {
    return conf;
  }

  @Override
  public void setRootPath(String path) {
    instance.setTemporaryPath(path);
  }

  @Override
  public String getRootPath() {
    return instance.getTemporaryPath();
  }

  @Override
  public void setKdc(KdcRunner kdc) {
    // Do nothing as KdcRunner is created by this class.
  }
}
