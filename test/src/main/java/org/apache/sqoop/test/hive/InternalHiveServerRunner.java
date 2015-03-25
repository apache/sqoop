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

import org.apache.hive.service.server.HiveServer2;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.utils.NetworkUtils;

/**
 * Use HiveServer2 JDBC client to run all operations.
 *
 * @see org.apache.sqoop.test.hive.HiveServerRunner
 */
public class InternalHiveServerRunner extends HiveServerRunner {
  private static final Logger LOG = Logger.getLogger(InternalHiveServerRunner.class);

  private final HiveServer2 hiveServer2;

  public InternalHiveServerRunner(String hostname, int port) throws Exception {
    super(hostname, port);
    hiveServer2 = new HiveServer2();
  }

  @Override
  public void start() throws Exception {
    Long start = System.currentTimeMillis();
    hiveServer2.init(getConfiguration());
    hiveServer2.start();
    NetworkUtils.waitForStartUp(getHostName(), getPort(), 5, 100);
    Long end = System.currentTimeMillis();
    LOG.debug("Hive service took " + (end - start)  + "ms to start");
  }

  @Override
  public void stop() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }
}
