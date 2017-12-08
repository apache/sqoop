package org.apache.sqoop.hive;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.Service;
import org.apache.hive.service.server.HiveServer2;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;

public class HiveMiniCluster {

  private static final String DEFAULT_HOST = "127.0.0.1";

  private static final int DEFAULT_PORT = 10000;

  private final String hostName;

  private final int port;

  private final String metastorePath;

  private final HiveServer2 hiveServer2;

  private HiveConf config;

  public HiveMiniCluster() {
    this(DEFAULT_HOST, DEFAULT_PORT);
  }

  public HiveMiniCluster(String hostname, int port) {
    this(hostname, port, Files.createTempDir().getAbsolutePath());
  }

  public HiveMiniCluster(String hostname, int port, String metastorePath) {
    this.hostName = hostname;
    this.port = port;
    this.metastorePath = metastorePath;
    this.hiveServer2 = new HiveServer2();
  }

  private HiveConf createHiveConf() {
    HiveConf result = new HiveConf();
    result.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, getHostName());
    result.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, getPort());
    result.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, getMetastoreConnectUrl());

    return result;
  }

  public void start() {
    try {
      config = createHiveConf();
      startHiveServer();
      NetworkUtils.waitForStartUp(getHostName(), getPort(), 5, 100);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startHiveServer() throws Exception {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    loginUser.doAs(new PrivilegedAction<Void>() {
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
    try {
      FileUtils.deleteDirectory(new File(metastorePath));
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
    return String.format("jdbc:hive2://%s:%d/default", hostName, port);
  }

  public String getMetastorePath() {
    return metastorePath;
  }

  public String getMetastoreConnectUrl() {
    return String.format("jdbc:derby:;databaseName=%s/minicluster_metastore_db;create=true", metastorePath);
  }

  public boolean isStarted() {
    return hiveServer2.getServiceState() == Service.STATE.STARTED;
  }

}
