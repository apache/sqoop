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
package org.apache.sqoop.test.testcases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.db.HiveProvider;
import org.apache.sqoop.test.hive.InternalHiveServerRunner;
import org.apache.sqoop.test.hive.HiveServerRunner;
import org.apache.sqoop.test.hive.HiveServerRunnerFactory;
import org.apache.sqoop.test.hive.InternalMetastoreServerRunner;
import org.apache.sqoop.test.hive.MetastoreServerRunner;
import org.apache.sqoop.test.hive.MetastoreServerRunnerFactory;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class HiveConnectorTestCase extends ConnectorTestCase {
  private static final Logger LOG = Logger.getLogger(HiveConnectorTestCase.class);

  protected HiveServerRunner hiveServerRunner;
  protected MetastoreServerRunner metastoreServerRunner;
  protected HiveProvider hiveProvider;

  private void ensureWarehouseDirectory(Configuration conf) throws Exception {
    String warehouseDirectory = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    StringBuilder dir = new StringBuilder();
    for (String part : warehouseDirectory.split("/")) {
      dir.append(part).append("/");
      Path path = new Path(dir.toString());
      if (!hdfsClient.exists(path)) {
        hdfsClient.mkdirs(path);
      }
    }
    hdfsClient.setPermission(new Path(dir.toString()), new FsPermission((short)01777));
  }

  @BeforeMethod(alwaysRun = true)
  public void startHive() throws Exception {
    String databasePath = HdfsUtils.joinPathFragments(getTemporaryPath(), "metastore_db");
    metastoreServerRunner = MetastoreServerRunnerFactory.getRunner(System.getProperties(), InternalMetastoreServerRunner.class);
    metastoreServerRunner.setConfiguration(metastoreServerRunner.prepareConfiguration(hadoopCluster.getConfiguration()));
    metastoreServerRunner.getConfiguration().set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
        "jdbc:derby:;databaseName=" + databasePath + ";create=true");
    ensureWarehouseDirectory(metastoreServerRunner.getConfiguration());
    LOG.info("Starting Metastore Server: " + metastoreServerRunner.getClass().getName());
    metastoreServerRunner.start();

    hiveServerRunner = HiveServerRunnerFactory.getRunner(System.getProperties(), InternalHiveServerRunner.class);
    hiveServerRunner.setConfiguration(hiveServerRunner.prepareConfiguration(metastoreServerRunner.getConfiguration()));
    LOG.info("Starting Hive Server: " + hiveServerRunner.getClass().getName());
    hiveServerRunner.start();

    LOG.info("Starting Hive Provider: " + provider.getClass().getName());
    hiveProvider = new HiveProvider(hiveServerRunner.getUrl());
    hiveProvider.start();
  }

  @AfterMethod(alwaysRun = true)
  public void stopHive() throws Exception {
    LOG.info("Stopping Hive Provider: " + provider.getClass().getName());
    hiveProvider.stop();

    LOG.info("Stopping Hive Server: " + hiveServerRunner.getClass().getName());
    hiveServerRunner.stop();

    LOG.info("Stopping Metastore Server: " + metastoreServerRunner.getClass().getName());
    metastoreServerRunner.stop();
  }
}
