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

import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.connector.kite.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KiteDatasetExecutor.class)
public class TestKiteToDestroyer {

  private KiteToDestroyer destroyer;

  private LinkConfiguration linkConfig;

  private ToJobConfiguration toJobConfig;

  private final String[] expectedUris = new String[]{"a", "b"};

  @org.mockito.Mock
  private KiteDatasetExecutor executorMock;

  @Before
  public void setUp() {
    initMocks(this);
    mockStatic(KiteDatasetExecutor.class);

    destroyer = new KiteToDestroyer() {
      @Override
      protected KiteDatasetExecutor getExecutor(String uri, Schema schema,
          FileFormat format) {
        return executorMock;
      }
    };

    linkConfig = new LinkConfiguration();
    toJobConfig = new ToJobConfiguration();
    toJobConfig.toJobConfig.uri = "dataset:file:/foo/bar";
    toJobConfig.toJobConfig.fileFormat = FileFormat.AVRO;
  }

  @Test
  public void testDestroyForSuccessfulJob() {
    // setup
    DestroyerContext context = new DestroyerContext(null, true, null);
    when(KiteDatasetExecutor.listTemporaryDatasetUris(toJobConfig.toJobConfig.uri))
        .thenReturn(expectedUris);

    // exercise
    destroyer.destroy(context, linkConfig, toJobConfig);

    // verify
    for (String uri : expectedUris) {
      verify(executorMock, times(1)).mergeDataset(uri);
    }
  }

  @Test
  public void testDestroyForFailedJob() {
    // setup
    DestroyerContext context = new DestroyerContext(null, false, null);
    when(KiteDatasetExecutor.listTemporaryDatasetUris(toJobConfig.toJobConfig.uri))
        .thenReturn(expectedUris);
    for (String uri : expectedUris) {
      when(KiteDatasetExecutor.deleteDataset(uri)).thenReturn(true);
    }

    // exercise
    destroyer.destroy(context, linkConfig, toJobConfig);

    // verify
    for (String uri : expectedUris) {
      verifyStatic(times(1));
      KiteDatasetExecutor.deleteDataset(uri);
    }
  }

}