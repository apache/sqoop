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

package org.apache.sqoop.connector.kite;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KiteDatasetExecutor.class)
public class TestKiteToInitializer {

  private KiteToInitializer initializer;

  @org.mockito.Mock
  private KiteDatasetExecutor executorMock;

  @Before
  public void setUp() {
    initMocks(this);
    mockStatic(KiteDatasetExecutor.class);

    initializer = new KiteToInitializer();
  }

  @Test
  public void testInitializePassed() {
    // setup
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration toJobConfig = new ToJobConfiguration();
    toJobConfig.toJobConfig.uri = "dataset:file:/ds/not/exist";
    when(KiteDatasetExecutor.datasetExists(toJobConfig.toJobConfig.uri))
        .thenReturn(false);

    // exercise
    initializer.initialize(null, linkConfig, toJobConfig);
  }

  @Test(expected = SqoopException.class)
  public void testInitializeFailed() {
    // setup
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration toJobConfig = new ToJobConfiguration();
    toJobConfig.toJobConfig.uri = "dataset:file:/ds/exist";
    when(KiteDatasetExecutor.datasetExists(toJobConfig.toJobConfig.uri))
        .thenReturn(true);

    // exercise
    initializer.initialize(null, linkConfig, toJobConfig);
  }

  @Test
  public void testGetSchema() {
    // exercise
    Schema schema = initializer.getSchema(null, null, null);

    // verify
    assertNotNull(schema);
    assertTrue(schema.isEmpty());
  }

}