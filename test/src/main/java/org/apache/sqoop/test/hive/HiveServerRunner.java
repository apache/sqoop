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

/**
 * Hive server runner for testing purpose.
 *
 * Runner provides methods for bootstrapping and using HiveServer2. This
 * should allow providing a HiveServer2 minicluster or using a real server.
 */
public abstract class HiveServerRunner {
  private static final Logger LOG = Logger.getLogger(HiveServerRunner.class);

  private final String hostname;
  private final int port;

  public HiveServerRunner(String hostname, int port) throws Exception {
    this.hostname = hostname;

    if (port == 0) {
      this.port = NetworkUtils.findAvailablePort();
    } else {
      this.port = port;
    }

    LOG.info("Hive Server will bind to port " + getPort());
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
   * Return JDBC URL to be used with HiveServer2 instance.
   *
   * @return String
   */
  public String getUrl() {
    return "jdbc:hive2://" + hostname + ":" + port + "/default";
  }

  /**
   * Prepare configuration object. This method should be called once before the
   * start method is called.
   *
   * @param config is the configuration object to prepare.
   */
  public Configuration prepareConfiguration(Configuration config) throws Exception {
    config.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, getHostName());
    config.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, getPort());
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
   * Port hive service will be on.
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
