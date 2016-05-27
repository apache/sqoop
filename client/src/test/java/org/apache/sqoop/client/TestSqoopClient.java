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
package org.apache.sqoop.client;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.sqoop.client.request.SqoopResourceRequests;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.utils.MapResourceBundle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestSqoopClient {

  SqoopResourceRequests resourceRequests;
  SqoopClient client;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    resourceRequests = mock(SqoopResourceRequests.class);
    client = new SqoopClient("my-cool-server");
    client.setSqoopRequests(resourceRequests);
  }

  /**
   * Retrieve connector information, request to bundle for same connector should
   * not require additional HTTP request.
   */
  @Test
  public void testGetConnectorByString() {
    when(resourceRequests.readConnector(null)).thenReturn(connectorBean(connector(1)));
    MConnector connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());
    assertEquals("A1", connector.getUniqueName());

    client.getConnectorConfigBundle("A1");

    verify(resourceRequests, times(0)).readConnector("A1");
    verify(resourceRequests, times(1)).readConnector(null);
  }

  /**
   * Retrieve connector bundle, request for metadata for same connector should
   * not require additional HTTP request.
   */
  @Test
  public void testGetConnectorBundle() {
    when(resourceRequests.readConnector("A1")).thenReturn(connectorBean(connector(1)));
    client.getConnectorConfigBundle("A1");

    MConnector connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());

    verify(resourceRequests, times(1)).readConnector("A1");
  }

  /**
   * Retrieve driverConfig information, request to driverConfig bundle should not
   * require additional HTTP request.
   */
  @Test
  public void testGetDriverConfig() {
    when(resourceRequests.readDriver()).thenReturn(driverBean(driver()));

    client.getDriverConfig();
    client.getDriverConfigBundle();

    verify(resourceRequests, times(1)).readDriver();
  }

  /**
   * Retrieve driverConfig bundle, request to driverConfig metadata should not
   * require additional HTTP request.
   */
  @Test
  public void testGetDriverConfigBundle() {
    when(resourceRequests.readDriver()).thenReturn(driverBean(driver()));

    client.getDriverConfigBundle();
    client.getDriverConfig();

    verify(resourceRequests, times(1)).readDriver();
  }

  /**
   * Getting all connectors at once should avoid any other HTTP request to
   * specific connectors.
   */
  @Test
  public void testGetConnectors() {
    MConnector connector;

    when(resourceRequests.readConnector(null)).thenReturn(connectorBean(connector(1), connector(2)));
    Collection<MConnector> connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    client.getConnectorConfigBundle("A1");
    connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());

    connector = client.getConnector("A2");
    client.getConnectorConfigBundle("A2");
    assertEquals(2, connector.getPersistenceId());

    connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());
    assertEquals("A1", connector.getUniqueName());

    connector = client.getConnector("A2");
    assertEquals(2, connector.getPersistenceId());
    assertEquals("A2", connector.getUniqueName());

    connector = client.getConnector("A3");
    assertNull(connector);

    verify(resourceRequests, times(1)).readConnector(null);
    verifyNoMoreInteractions(resourceRequests);
  }


  /**
   * Getting connectors one by one should not be equivalent to getting all connectors
   * at once as Client do not know how many connectors server have.
   */
  @Test
  public void testGetConnectorOneByOne() {
    ConnectorBean bean = connectorBean(connector(1), connector(2));
    when(resourceRequests.readConnector(null)).thenReturn(bean);
    when(resourceRequests.readConnector("A1")).thenReturn(bean);
    when(resourceRequests.readConnector("A2")).thenReturn(bean);

    client.getConnectorConfigBundle("A1");
    client.getConnector("A1");

    client.getConnector("A2");
    client.getConnectorConfigBundle("A2");

    Collection<MConnector> connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    verify(resourceRequests, times(1)).readConnector(null);
    verify(resourceRequests, times(1)).readConnector("A1");
    verify(resourceRequests, times(0)).readConnector("A2");
    verifyNoMoreInteractions(resourceRequests);
  }

  @Test
  public void testGetConnectorsByDirection() {
    Collection<MConnector> filteredConnectors;

    MConnector connectorFrom = new MConnector("from_connector", "", "",
        new MLinkConfig(null, null),
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        null);

    MConnector connectorTo = new MConnector("to_connector", "", "",
        new MLinkConfig(null, null),
        null,
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));

    MConnector connectorBothWays = new MConnector("both_ways_connector", "", "",
        new MLinkConfig(null, null),
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));

    when(resourceRequests.readConnector(null)).thenReturn(connectorBean(connectorFrom, connectorTo, connectorBothWays));

    filteredConnectors = client.getConnectorsByDirection(Direction.FROM);
    assertTrue(filteredConnectors.contains(connectorFrom));
    assertTrue(!filteredConnectors.contains(connectorTo));
    assertTrue(filteredConnectors.contains(connectorBothWays));

    filteredConnectors = client.getConnectorsByDirection(Direction.TO);
    assertTrue(!filteredConnectors.contains(connectorFrom));
    assertTrue(filteredConnectors.contains(connectorTo));
    assertTrue(filteredConnectors.contains(connectorBothWays));
  }

  /**
   * Link for non-existing connector can't be created.
   */
  @Test(expectedExceptions = SqoopException.class)
  public void testCreateLink() {
    when(resourceRequests.readConnector(null)).thenReturn(connectorBean(connector(1)));
    client.createLink("non existing connector");
  }

  private ConnectorBean connectorBean(MConnector...connectors) {
    List<MConnector> connectorList = new ArrayList<MConnector>();
    Map<String, ResourceBundle> bundles = new HashMap<String, ResourceBundle>();

    for(MConnector connector : connectors) {
      connectorList.add(connector);
      bundles.put(connector.getUniqueName(), null);
    }
    return new ConnectorBean(connectorList, bundles);
  }
  private DriverBean driverBean(MDriver driver) {
    return new DriverBean(driver, new MapResourceBundle(null));
  }

  private MConnector connector(long id) {
    MConnector connector = new MConnector("A" + id, "A" + id, "1.0" + id,
        new MLinkConfig(null, null), new MFromConfig(null, null), new MToConfig(null, null));
    connector.setPersistenceId(id);
    return connector;
  }

  private MDriver driver() {
    MDriver driver = new MDriver(new MDriverConfig(new LinkedList<MConfig>(), new LinkedList<MValidator>()), "1");
    driver.setPersistenceId(1);
    return driver;
  }
}
