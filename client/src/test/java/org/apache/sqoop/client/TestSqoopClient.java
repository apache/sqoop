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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNull;
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
  public void testGetConnector() {
    when(resourceRequests.readConnector(1L)).thenReturn(connectorBean(connector(1)));
    MConnector connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    client.getConnectorConfigBundle(1L);

    verify(resourceRequests, times(1)).readConnector(1L);
  }

  @Test
  public void testGetConnectorByString() {
    when(resourceRequests.readConnector(null)).thenReturn(connectorBean(connector(1)));
    MConnector connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());
    assertEquals("A1", connector.getUniqueName());

    client.getConnectorConfigBundle(1L);

    verify(resourceRequests, times(0)).readConnector(1L);
    verify(resourceRequests, times(1)).readConnector(null);
  }

  /**
   * Retrieve connector bundle, request for metadata for same connector should
   * not require additional HTTP request.
   */
  @Test
  public void testGetConnectorBundle() {
    when(resourceRequests.readConnector(1L)).thenReturn(connectorBean(connector(1)));
    client.getConnectorConfigBundle(1L);

    MConnector connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    verify(resourceRequests, times(1)).readConnector(1L);
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

    client.getConnectorConfigBundle(1);
    connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    connector = client.getConnector(2);
    client.getConnectorConfigBundle(2);
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
    when(resourceRequests.readConnector(1L)).thenReturn(bean);
    when(resourceRequests.readConnector(2L)).thenReturn(bean);

    client.getConnectorConfigBundle(1);
    client.getConnector(1);

    client.getConnector(2);
    client.getConnectorConfigBundle(2);

    Collection<MConnector> connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    verify(resourceRequests, times(1)).readConnector(null);
    verify(resourceRequests, times(1)).readConnector(1L);
    verify(resourceRequests, times(1)).readConnector(2L);
    verifyNoMoreInteractions(resourceRequests);
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
    Map<Long, ResourceBundle> bundles = new HashMap<Long, ResourceBundle>();

    for(MConnector connector : connectors) {
      connectorList.add(connector);
      bundles.put(connector.getPersistenceId(), null);
    }
    return new ConnectorBean(connectorList, bundles);
  }
  private DriverBean driverBean(MDriver driver) {
    return new DriverBean(driver, new MapResourceBundle(null));
  }

  private MConnector connector(long id) {
    MConnector connector = new MConnector("A" + id, "A" + id, "1.0" + id,
        new MLinkConfig(null), new MFromConfig(null), new MToConfig(null));
    connector.setPersistenceId(id);
    return connector;
  }

  private MDriver driver() {
    MDriver driver = new MDriver(new MDriverConfig(new LinkedList<MConfig>()), "1");
    driver.setPersistenceId(1);
    return driver;
  }
}
