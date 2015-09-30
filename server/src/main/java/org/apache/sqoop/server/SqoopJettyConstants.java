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

import org.apache.sqoop.core.ConfigurationConstants;

public class SqoopJettyConstants {

  /**
   * All audit logger related configuration is prefixed with this:
   * <tt>org.apache.sqoop.jetty.</tt>
   */
  public static final String PREFIX_JETTY_CONFIG =
          ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "jetty.";

  /**
   * Max number of worker in thread pool to be used, specified by:
   * <tt>org.apache.sqoop.jetty.thread.pool.worker.max</tt>
   */
  public static final String SYSCFG_JETTY_THREAD_POOL_MAX_WORKER = PREFIX_JETTY_CONFIG
          + "thread.pool.worker.max";

  public static final int DEFAULT_JETTY_THREAD_POOL_MAX_WORKER = 500;

  /**
   * Min number of worker in thread pool to be used, specified by:
   * <tt>org.apache.sqoop.jetty.thread.pool.worker.min</tt>
   */
  public static final String SYSCFG_JETTY_THREAD_POOL_MIN_WORKER = PREFIX_JETTY_CONFIG
          + "thread.pool.worker.min";

  public static final int DEFAULT_JETTY_THREAD_POOL_MIN_WORKER = 5;

  /**
   * Alive time of Worker in thread pool to be used, specified by:
   * <tt>org.apache.sqoop.jetty.thread.pool.worker.alive.time</tt>
   */
  public static final String SYSCFG_JETTY_THREAD_POOL_WORKER_ALIVE_TIME = PREFIX_JETTY_CONFIG
          + "thread.pool.worker.alive.time";

  public static final long DEFAULT_JETTY_THREAD_POOL_WORKER_ALIVE_TIME = 60;

  /**
   * The Port number for Jetty server:
   * <tt>org.apache.sqoop.jetty.port</tt>
   */
  public static final String SYSCFG_JETTY_PORT = PREFIX_JETTY_CONFIG + "port";

  public static final int DEFAULT_SYSCFG_JETTY_PORT = 12000;
}
