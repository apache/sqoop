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
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;

/**
 * Database infrastructure provider.
 */
public class DatabaseInfrastructureProvider extends InfrastructureProvider {
  private static final Logger LOG = Logger.getLogger(DatabaseInfrastructureProvider.class);

  private DatabaseProvider instance;
  private Configuration conf;

  public DatabaseInfrastructureProvider() {
    try {
      instance = DatabaseProviderFactory.getProvider(System.getProperties());
    } catch (Exception e) {
      LOG.error("Error fetching Hadoop runner.", e);
    }
  }

  @Override
  public void start() {
    instance.start();
  }

  @Override
  public void stop() {
    instance.stop();
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
    // No-op.
  }

  @Override
  public String getRootPath() {
    return null;
  }

  public DatabaseProvider getInstance() {
    return instance;
  }
}
