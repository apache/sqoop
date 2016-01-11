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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.kafka.TestUtil;
import org.apache.sqoop.test.kdc.KdcRunner;

public class KafkaInfrastructureProvider extends InfrastructureProvider {

  private static final Logger LOG = Logger.getLogger(KafkaInfrastructureProvider.class);

  private static TestUtil testUtil = TestUtil.getInstance();
  protected String topic;

  @Override
  public void start() {
    // starts Kafka server and its dependent zookeeper
    try {
      testUtil.prepare();
    } catch (Exception e) {
      LOG.error("Error starting kafka.", e);
    }
  }

  @Override
  public void stop() {
    try {
      testUtil.tearDown();
    } catch (IOException e) {
      LOG.error("Error stopping kafka.", e);
    }
  }

  @Override
  public void setHadoopConfiguration(Configuration conf) {
    // do nothing
  }

  @Override
  public Configuration getHadoopConfiguration() {
    return null;
  }

  @Override
  public void setRootPath(String path) {
    // do nothing
  }

  @Override
  public String getRootPath() {
    return null;
  }

  @Override
  public void setKdc(KdcRunner kdc) {
    // do nothing
  }

}
