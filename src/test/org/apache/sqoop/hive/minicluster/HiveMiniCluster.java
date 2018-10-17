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

package org.apache.sqoop.hive.minicluster;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HiveMiniCluster {

  private static final Log LOG = LogFactory.getLog(HiveMiniCluster.class.getName());

  private static final String DEFAULT_HOST = "127.0.0.1";

  private static final int DEFAULT_PORT = 10000;

  private final String hostName;

  private final int port;

  private final String tempFolderPath;

  private final AuthenticationConfiguration authenticationConfiguration;

  private final HiveServer2 hiveServer2;

  private HiveConf config;

  private URL originalHiveSiteLocation;

  public HiveMiniCluster(AuthenticationConfiguration authenticationConfiguration) {
    this(DEFAULT_HOST, DEFAULT_PORT, authenticationConfiguration);
  }

  public HiveMiniCluster(String hostname, int port, AuthenticationConfiguration authenticationConfiguration) {
    this(hostname, port, Files.createTempDir().getAbsolutePath(), authenticationConfiguration);
  }

  public HiveMiniCluster(String hostname, int port, String tempFolderPath, AuthenticationConfiguration authenticationConfiguration) {
    this.hostName = hostname;
    this.port = port;
    this.tempFolderPath = tempFolderPath;
    this.authenticationConfiguration = authenticationConfiguration;
    this.hiveServer2 = new HiveServer2();
  }

  private void createHiveConf() {
    config = new HiveConf();
    config.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, tempFolderPath);
    config.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, getHostName());
    config.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, getPort());
    config.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, getMetastoreConnectUrl());
    // setting port to -1 to turn the webui off
    config.setInt(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, -1);

    for (Map.Entry<String, String> authConfig : authenticationConfiguration.getAuthenticationConfig().entrySet()) {
      config.set(authConfig.getKey(), authConfig.getValue());
    }
  }

  public void start() {
    try {
      authenticationConfiguration.init();
      createHiveConf();
      createHiveSiteXml();
      startHiveServer();
      waitForStartUp();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createHiveSiteXml() throws IOException {
    File hiveSiteXmlFile = new File(tempFolderPath,"hive-site.xml");
    try (OutputStream out = new FileOutputStream(hiveSiteXmlFile)) {
      config.writeXml(out);
    }

    originalHiveSiteLocation = HiveConf.getHiveSiteLocation();
    HiveConf.setHiveSiteLocation(hiveSiteXmlFile.toURI().toURL());
  }

  private void startHiveServer() throws Exception {
    authenticationConfiguration.doAsAuthenticated(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        hiveServer2.init(config);
        hiveServer2.start();
        return null;
      }
    });
  }

  public void stop() {
    hiveServer2.stop();
    HiveConf.setHiveSiteLocation(originalHiveSiteLocation);
    try {
      FileUtils.deleteDirectory(new File(tempFolderPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public HiveConf getConfig() {
    return config;
  }

  public int getPort() {
    return port;
  }

  public String getHostName() {
    return hostName;
  }

  public String getUrl() {
    return String.format("jdbc:hive2://%s:%d/default%s", hostName, port, authenticationConfiguration.getUrlParams());
  }

  public String getTempFolderPath() {
    return tempFolderPath;
  }

  public String getMetastoreConnectUrl() {
    return String.format("jdbc:derby:;databaseName=%s/minicluster_metastore_db;create=true", tempFolderPath);
  }

  public boolean isStarted() {
    return hiveServer2.getServiceState() == Service.STATE.STARTED;
  }

  private void waitForStartUp() throws InterruptedException, TimeoutException {
    final int numberOfAttempts = 500;
    final long sleepTime = 100;
    for (int i = 0; i < numberOfAttempts; ++i) {
      try {
        LOG.debug("Attempt " + (i + 1) + " to access " + hostName + ":" + port);
        new Socket(InetAddress.getByName(hostName), port).close();
        return;
      } catch (RuntimeException | IOException e) {
        LOG.debug("Failed to connect to " + hostName + ":" + port, e);
      }

      Thread.sleep(sleepTime);
    }

    throw new RuntimeException("Couldn't access new server: " + hostName + ":" + port);
  }

}
