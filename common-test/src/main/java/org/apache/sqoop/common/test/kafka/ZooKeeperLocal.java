/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.sqoop.common.test.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class ZooKeeperLocal {
  private int zkPort;
  private ZooKeeperServer zookeeper;
  private NIOServerCnxnFactory factory;
  File dir;


  public ZooKeeperLocal(int zkPort){
    int numConnections = 5000;
    int tickTime = 2000;

    this.zkPort = zkPort;

    String dataDirectory = "target";
    dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

    try {
      this.zookeeper = new ZooKeeperServer(dir,dir,tickTime);
      this.factory = new NIOServerCnxnFactory();
      factory.configure(new InetSocketAddress("127.0.0.1",zkPort),0);
      factory.startup(zookeeper);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void stopZookeeper() throws IOException {
    zookeeper.shutdown();
    factory.shutdown();
    FileUtils.deleteDirectory(dir);
  }

  public String getConnectString() {
    return "127.0.0.1:"+zkPort;
  }
}
