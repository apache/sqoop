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
package org.apache.sqoop.common.test.db;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.sqoop.common.test.db.types.DatabaseTypeList;
import org.apache.sqoop.common.test.db.types.DerbyTypeList;
import org.apache.sqoop.common.test.utils.LoggerWriter;
import org.apache.sqoop.common.test.utils.NetworkUtils;

import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of database provider that is based on embedded derby server.
 *
 * This provider will work out of the box without any extra configuration.
 */
public class DerbyProvider extends DatabaseProvider {

  private static final Logger LOG = Logger.getLogger(DerbyProvider.class);

  public static final String DRIVER = "org.apache.derby.jdbc.ClientDriver";

  // Used port for this instance
  int port;

  NetworkServerControl server = null;

  // We've observed several cases where Derby did not start properly
  // from various reasons without any Exception being raised and any
  // subsequent call to server.ping() or server.stop() got the process
  // into zombie state waiting forever. Hence we're having boolean
  // variable that is guarding potentially dangerous calls.
  boolean started = false;

  @Override
  public void start() {
    // Start embedded server
    try {
      port = NetworkUtils.findAvailablePort();
      LOG.info("Will bind to port " + port);

      server = new NetworkServerControl(InetAddress.getByName("localhost"), port);
      server.start(new LoggerWriter(LOG, Level.INFO));

      // Start won't thrown an exception in case that it fails to start, one
      // have to explicitly call ping() in order to verify if the server is
      // up. Check DERBY-1465 for more details.
      //
      // In addition we've observed that in some scenarios ping() can get into
      // deadlock waiting on remote server forever and hence we're having
      // our own timeout handling around it.
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future future = executorService.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          while (true) {
            try {
              server.ping();
              break;
            } catch (Exception e) {
              LOG.warn("Could not ping derby server on port " + port, e);
            }

            Thread.sleep(1000);
          }

          return null;
        }
      });
      future.get(10, TimeUnit.SECONDS);

      // Server successfully started at this point
      started = true;
    } catch (Exception e) {
      String message = "Can't start embedded Derby server";
      LOG.fatal(message, e);
      throw new RuntimeException(message, e);
    }

    super.start();
  }

  @Override
  public void stop() {
    super.stop();

    // Shutdown embedded server
    try {
      if(started) {
        server.shutdown();
      }
    } catch (Exception e) {
      String message = "Can't shut down embedded Derby server";
      LOG.fatal(message, e);
      throw new RuntimeException(message, e);
    }
  }

  @Override
  public String escapeColumnName(String columnName) {
    return escape(columnName);
  }

  @Override
  public String escapeTableName(String tableName) {
    return escape(tableName);
  }

  @Override
  public String escapeSchemaName(String schemaName) {
    return escape(schemaName);
  }

  @Override
  public String escapeValueString(String value) {
    return "'" + value + "'";
  }

  @Override
  public boolean isSupportingScheme() {
    return true;
  }

  public String escape(String entity) {
    return "\"" + entity + "\"";
  }

  @Override
  public String getJdbcDriver() {
    return DRIVER;
  }

  @Override
  public String getConnectionUrl() {
    return "jdbc:derby://localhost:" + port + "/memory:sqoop;create=true";
  }

  @Override
  public String getConnectionUsername() {
    return null;
  }

  @Override
  public String getConnectionPassword() {
    return null;
  }

  @Override
  public DatabaseTypeList getDatabaseTypes() {
    return new DerbyTypeList();
  }
}
