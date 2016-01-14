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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * A connector that does not do anything
 */
public class TestConnectorForShell extends SqoopConnector {

  private static final From FROM = new From(
          TestFromInitializerForShell.class,
          TestPartitionerForShell.class,
          TestPartitionForShell.class,
          TestExtractorForShell.class,
          TestFromDestroyerForShell.class);

  private static final To TO = new To(TestToInitializerForShell.class,
    TestLoaderForShell.class,
    TestToDestroyerForShell.class);

  @Override
  public String getVersion() {
    return "1.0";
  }

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle("test-connector-for-shell", locale);
  }

  @Override
  public Class getLinkConfigurationClass() {
    return TestLinkConfigurationForShell.class;
  }

  @Override
  public Class getJobConfigurationClass(Direction direction) {
    switch (direction) {
      case FROM:
        return TestFromJobConfigurationForShell.class;
      case TO:
        return TestToJobConfigurationForShell.class;
      default:
        return null;
    }
  }

  @Override
  public From getFrom() {
    return FROM;
  }

  @Override
  public To getTo() {
    return TO;
  }

  @Override
  public ConnectorConfigurableUpgrader getConfigurableUpgrader(String oldConnectorVersion) {
    return null;
  }
}