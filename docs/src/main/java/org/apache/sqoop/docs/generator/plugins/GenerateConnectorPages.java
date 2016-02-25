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
package org.apache.sqoop.docs.generator.plugins;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.ConnectorHandler;
import org.apache.sqoop.connector.ConnectorManagerUtils;
import org.apache.sqoop.connector.spi.SqoopConnector;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Collections;
import java.util.List;

public class GenerateConnectorPages extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(GenerateConnectorPages.class);

  @Override
  public void run() {
    try {
      runWithException();
    } catch (IOException e) {
      throw new RuntimeException("Can't generate connector documentation", e);
    }
  }

  public void runWithException() throws IOException {
    List<URL> connectorUrls = ConnectorManagerUtils.getConnectorConfigs(Collections.<String>emptySet());
    for (URL url : connectorUrls) {
      SqoopConnector connector = new ConnectorHandler(url).getSqoopConnector();
      LOG.info("Loaded " +  connector.getConnectorName() + " from  " + url.toString());

      String connectorName = connector.getConnectorName();

      File outputFile = FileUtils.getFile(getDestination(), "user", "connectors", connectorName + ".rst");
      PrintWriter writer = new PrintWriter(outputFile);

      // Writing headline
      writer.println(connectorName);
      writer.println(StringUtils.repeat("=", connectorName.length()));

      writer.close();
    }
  }
}
