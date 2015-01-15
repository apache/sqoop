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
import org.kitesdk.data.Datasets;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.powermock.core.classloader.annotations.PrepareForTest;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@PrepareForTest(Datasets.class)
@PowerMockIgnore("org.apache.sqoop.common.ErrorCode")
public class TestKiteToInitializer extends PowerMockTestCase {

  private KiteToInitializer initializer;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    initMocks(this);
    mockStatic(Datasets.class);

    initializer = new KiteToInitializer();
  }

  @Test
  public void testInitializePassed() {
    // setup
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration toJobConfig = new ToJobConfiguration();
    toJobConfig.toJobConfig.uri = "dataset:file:/ds/not/exist";
    when(Datasets.exists(toJobConfig.toJobConfig.uri))
        .thenReturn(false);

    // exercise
    initializer.initialize(null, linkConfig, toJobConfig);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testInitializeFailed() {
    // setup
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration toJobConfig = new ToJobConfiguration();
    toJobConfig.toJobConfig.uri = "dataset:file:/ds/exist";
    when(Datasets.exists(toJobConfig.toJobConfig.uri))
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
