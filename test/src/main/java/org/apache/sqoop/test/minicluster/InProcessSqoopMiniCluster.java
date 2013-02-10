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
import org.apache.sqoop.core.SqoopServer;

/**
 * In process Sqoop server mini cluster.
 *
 * This class will create and initialize Sqoop mini cluster that will be accessible
 * using usual manager calls and running inside one single thread. Created server
 * won't be accessible over HTTP.
 */
public class InProcessSqoopMiniCluster extends SqoopMiniCluster {

  /** {@inheritDoc} */
  public InProcessSqoopMiniCluster(String temporaryPath) throws Exception {
    super(temporaryPath);
  }

  /** {@inheritDoc} */
  public InProcessSqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
    super(temporaryPath, configuration);
  }

  /** {@inheritDoc} */
  @Override
  public void start() throws Exception {
    prepareTemporaryPath();
    SqoopServer.initialize();
  }

  /** {@inheritDoc} */
  @Override
  public void stop() throws Exception {
    SqoopServer.destroy();
  }

}
