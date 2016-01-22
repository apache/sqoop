/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * limitations under the License.
 */

package org.apache.sqoop.common.test.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ZooKeeperLocal {
    private int zkPort;
    private ZooKeeperServer zookeeper;
    private NIOServerCnxnFactory factory;
    private static String ZOOKEEPER_HOSTS= "";
    File dir;
    private static Config conf;

    public ZooKeeperLocal(int zkPort) {
        conf = ConfigFactory.load();
        ZOOKEEPER_HOSTS= StringUtils.join(conf.getStringList("zookeeper.hosts"), ",");
        int numConnections = 5000;
        int tickTime = 2000;

        this.zkPort = zkPort;

        String dataDirectory = "target";
        dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

        try {
            this.zookeeper = new ZooKeeperServer(dir, dir, tickTime);
            this.factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress(ZOOKEEPER_HOSTS, zkPort), 0);
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
        return ZOOKEEPER_HOSTS + zkPort;
    }
}
