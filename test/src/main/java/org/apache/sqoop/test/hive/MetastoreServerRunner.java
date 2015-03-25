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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.utils.NetworkUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;

/**
 * Metastore server runner for testing purpose.
 *
 * Runner provides methods for bootstrapping and using the Hive metastore. This
 * should allow providing a Hive metastore minicluster or using a real server.
 */
public abstract class MetastoreServerRunner {
  private static final Logger LOG = Logger.getLogger(MetastoreServerRunner.class);

  private final String hostname;
  private final int port;
  private final String warehouseDirectory;

  public MetastoreServerRunner(String hostname, int port) throws Exception {
    this.hostname = hostname;
    this.warehouseDirectory = "/user/hive/" + UUID.randomUUID();

    if (port == 0) {
      port = NetworkUtils.findAvailablePort();
    }
    LOG.info("Hive Metastore will bind to port " + port);

    this.port = port;
  }

  /**
   * Configuration object.
   */
  protected HiveConf config = null;

  /**
   * Start Hive server.
   *
   * @throws Exception
   */
  abstract public void start() throws Exception;

  /**
   * Stop Hive server.
   *
   * @throws Exception
   */
  abstract public void stop() throws Exception;

  /**
   * Metastore URI authority (ex. hostname:port).
   *
   * @return String metastore authority.
   */
  public String getAuthority() {
    return getHostName() + ":" + getPort();
  }

  /**
   * Prepare configuration object. This method should be called once before the
   * start method is called.
   *
   * @param config is the configuration object to prepare.
   */
  public Configuration prepareConfiguration(Configuration config) throws Exception {
    config.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://" + getHostName() + ":" + getPort());
    config.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDirectory);
    return config;
  }

  /**
   * Get configuration.
   *
   * @return HiveConf
   */
  public HiveConf getConfiguration() {
    return config;
  }

  /**
   * Set the configuration object.
   *
   * @param config
   */
  public void setConfiguration(Configuration config) {
    this.config = new HiveConf();
    this.config.addResource(config);
    this.printConfig();
  }

  /**
   * Hostname used to start services on.
   *
   * @return String hostname
   */
  public String getHostName() {
    return hostname;
  }

  /**
   * Port meta store service will be on.
   *
   * @return int port
   */
  public int getPort() {
    return this.port;
  }

  private void printConfig() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    config.logVars(new PrintStream(baos));
    LOG.debug("Hive server runner configuration:\n" + baos.toString());
  }
}
