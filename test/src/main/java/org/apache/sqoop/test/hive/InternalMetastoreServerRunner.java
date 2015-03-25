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
package org.apache.sqoop.test.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.utils.NetworkUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Start a Hive Metastore service on the specified hostname and port.
 *
 * @see org.apache.sqoop.test.hive.MetastoreServerRunner
 */
public class InternalMetastoreServerRunner extends MetastoreServerRunner {
  private static final Logger LOG = Logger.getLogger(InternalMetastoreServerRunner.class);

  private ExecutorService executor = Executors
      .newSingleThreadExecutor();

  public InternalMetastoreServerRunner(String hostname, int port) throws Exception {
    super(hostname, port);
  }

  @Override
  public void start() throws Exception {
    Long start = System.currentTimeMillis();
    final int metastorePort = getPort();
    final HiveConf conf = getConfiguration();
    Callable<Void> metastoreService = new Callable<Void>() {
      public Void call() throws Exception {
        try {
          HiveMetaStore.startMetaStore(metastorePort,
              ShimLoader.getHadoopThriftAuthBridge(), conf);
          while(true);
        } catch (Throwable e) {
          throw new Exception("Error starting metastore", e);
        }
      }
    };
    executor.submit(metastoreService);
    NetworkUtils.waitForStartUp(getHostName(), getPort(), 5, 1000);
    Long end = System.currentTimeMillis();
    LOG.debug("Metastore service took " + (end - start)  + "ms to start");
  }

  @Override
  public void stop() throws Exception {
    executor.shutdownNow();
  }
}
