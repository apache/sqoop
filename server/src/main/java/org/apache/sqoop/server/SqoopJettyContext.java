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

package org.apache.sqoop.server;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;

public class SqoopJettyContext {
  private static final Logger LOG =
          Logger.getLogger(SqoopJettyContext.class);

  private final MapContext context;
  private final int minWorkerThreads;
  private final int maxWorkerThreads;
  private final int port;
  private final long workerKeepAliveTime;

  public SqoopJettyContext(MapContext context) {
    this.context = context;
    port = context.getInt(SqoopJettyConstants.SYSCFG_JETTY_PORT,
            SqoopJettyConstants.DEFAULT_SYSCFG_JETTY_PORT);
    minWorkerThreads = context.getInt(SqoopJettyConstants.SYSCFG_JETTY_THREAD_POOL_MIN_WORKER,
            SqoopJettyConstants.DEFAULT_JETTY_THREAD_POOL_MIN_WORKER);
    maxWorkerThreads = context.getInt(SqoopJettyConstants.SYSCFG_JETTY_THREAD_POOL_MAX_WORKER,
            SqoopJettyConstants.DEFAULT_JETTY_THREAD_POOL_MAX_WORKER);
    workerKeepAliveTime = context.getLong(SqoopJettyConstants.SYSCFG_JETTY_THREAD_POOL_WORKER_ALIVE_TIME,
            SqoopJettyConstants.DEFAULT_JETTY_THREAD_POOL_WORKER_ALIVE_TIME);
  }

  public int getMinWorkerThreads() {
    return minWorkerThreads;
  }

  public int getMaxWorkerThreads() {
    return maxWorkerThreads;
  }

  public int getPort() {
    return port;
  }

  public long getWorkerKeepAliveTime() {
    return workerKeepAliveTime;
  }
}
