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
import org.apache.sqoop.connector.kite.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.kitesdk.data.Datasets;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@PrepareForTest(Datasets.class)
@PowerMockIgnore("org.apache.sqoop.common.ErrorCode")
public class TestKiteFromInitializer extends PowerMockTestCase {

  private KiteFromInitializer initializer;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    initMocks(this);
    mockStatic(Datasets.class);

    initializer = new KiteFromInitializer();
  }

  @Test
  public void testInitializePassed() {
    // setup
    FromJobConfiguration jobConfig = new FromJobConfiguration();
    jobConfig.fromJobConfig.uri = "dataset:file:/ds/exist";
    when(Datasets.exists(jobConfig.fromJobConfig.uri)).thenReturn(true);

    // exercise
    initializer.initialize(null, new LinkConfiguration(), jobConfig);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testInitializeFailed() {
    // setup
    FromJobConfiguration jobConfig = new FromJobConfiguration();
    jobConfig.fromJobConfig.uri = "dataset:file:/ds/not/exist";
    when(Datasets.exists(jobConfig.fromJobConfig.uri)).thenReturn(false);

    // exercise
    initializer.initialize(null, new LinkConfiguration(), jobConfig);
  }

}