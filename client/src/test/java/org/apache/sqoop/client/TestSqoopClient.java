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

import org.apache.sqoop.client.request.SqoopRequests;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.utils.MapResourceBundle;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class TestSqoopClient {

  SqoopRequests requests;
  SqoopClient client;

  @Before
  public void setUp() {
    requests = mock(SqoopRequests.class);
    client = new SqoopClient("my-cool-server");
    client.setSqoopRequests(requests);
  }

  /**
   * Retrieve connector information, request to bundle for same connector should
   * not require additional HTTP request.
   */
  @Test
  public void testGetConnector() {
    when(requests.readConnector(1L)).thenReturn(connectorBean(connector(1)));
    MConnector connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    client.getResourceBundle(1L);

    verify(requests, times(1)).readConnector(1L);
  }

  @Test
  public void testGetConnectorByString() {
    when(requests.readConnector(null)).thenReturn(connectorBean(connector(1)));
    MConnector connector = client.getConnector("A1");
    assertEquals(1, connector.getPersistenceId());
    assertEquals("A1", connector.getUniqueName());

    client.getResourceBundle(1L);

    verify(requests, times(0)).readConnector(1L);
    verify(requests, times(1)).readConnector(null);
  }

  /**
   * Retrieve connector bundle, request for metadata for same connector should
   * not require additional HTTP request.
   */
  @Test
  public void testGetConnectorBundle() {
    when(requests.readConnector(1L)).thenReturn(connectorBean(connector(1)));
    client.getResourceBundle(1L);

    MConnector connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    verify(requests, times(1)).readConnector(1L);
  }

  /**
   * Retrieve framework information, request to framework bundle should not
   * require additional HTTP request.
   */
  @Test
  public void testGetFramework() {
    when(requests.readFramework()).thenReturn(frameworkBean(framework()));

    client.getFramework();
    client.getFrameworkResourceBundle();

    verify(requests, times(1)).readFramework();
  }

  /**
   * Retrieve framework bundle, request to framework metadata should not
   * require additional HTTP request.
   */
  @Test
  public void testGetFrameworkBundle() {
    when(requests.readFramework()).thenReturn(frameworkBean(framework()));

    client.getFrameworkResourceBundle();
    client.getFramework();

    verify(requests, times(1)).readFramework();
  }

  /**
   * Getting all connectors at once should avoid any other HTTP request to
   * specific connectors.
   */
  @Test
  public void testGetConnectors() {
    MConnector connector;

    when(requests.readConnector(null)).thenReturn(connectorBean(connector(1), connector(2)));
    Collection<MConnector> connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    client.getResourceBundle(1);
    connector = client.getConnector(1);
    assertEquals(1, connector.getPersistenceId());

    connector = client.getConnector(2);
    client.getResourceBundle(2);
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

    verify(requests, times(1)).readConnector(null);
    verifyNoMoreInteractions(requests);
  }


  /**
   * Getting connectors one by one should not be equivalent to getting all connectors
   * at once as Client do not know how many connectors server have.
   */
  @Test
  public void testGetConnectorOneByOne() {
    ConnectorBean bean = connectorBean(connector(1), connector(2));
    when(requests.readConnector(null)).thenReturn(bean);
    when(requests.readConnector(1L)).thenReturn(bean);
    when(requests.readConnector(2L)).thenReturn(bean);

    client.getResourceBundle(1);
    client.getConnector(1);

    client.getConnector(2);
    client.getResourceBundle(2);

    Collection<MConnector> connectors = client.getConnectors();
    assertEquals(2, connectors.size());

    verify(requests, times(1)).readConnector(null);
    verify(requests, times(1)).readConnector(1L);
    verify(requests, times(1)).readConnector(2L);
    verifyNoMoreInteractions(requests);
  }

  /**
   * Connection for non-existing connector can't be created.
   */
  @Test(expected = SqoopException.class)
  public void testNewConnection() {
    when(requests.readConnector(null)).thenReturn(connectorBean(connector(1)));
    client.newConnection("non existing connector");
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
  private FrameworkBean frameworkBean(MFramework framework) {
    return new FrameworkBean(framework, new MapResourceBundle(null));
  }

  private MConnector connector(long id) {
    MConnector connector = new MConnector("A" + id, "A" + id, "1.0" + id,
        new MConnectionForms(null), new MJobForms(null), new MJobForms(null));
    connector.setPersistenceId(id);
    return connector;
  }

  private MFramework framework() {
    MFramework framework = new MFramework(new MConnectionForms(null),
        new MJobForms(null), "1");
    framework.setPersistenceId(1);
    return framework;
  }
}
