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
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestKiteLoader {

  private KiteLoader loader;

  @org.mockito.Mock
  private KiteDatasetExecutor executorMock;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    initMocks(this);

    loader = new KiteLoader() {
      @Override
      protected KiteDatasetExecutor getExecutor(LinkConfiguration linkConfiguration, String uri, Schema schema,
          FileFormat format) {
        return executorMock;
      }
    };
  }

  @Test
  public void testLoader() throws Exception {
    // setup
    final int NUMBER_OF_ROWS = 1000;
    org.apache.sqoop.schema.Schema schema =
        new org.apache.sqoop.schema.Schema("TestLoader");
    schema.addColumn(new Text("TextCol"));
    DataReader reader = new DataReader() {
      private long index = 0L;
      @Override
      public Object[] readArrayRecord() {
        if (index++ < NUMBER_OF_ROWS) {
          return new Object[]{
              Long.toString(index),
          };
        } else {
          return null;
        }
      }
      @Override
      public String readTextRecord() {
        return null;
      }
      @Override
      public Object readContent() {
        return null;
      }
    };
    LoaderContext context = new LoaderContext(null, reader, schema);
    LinkConfiguration linkConfig = new LinkConfiguration();
    ToJobConfiguration toJobConfig = new ToJobConfiguration();

    // exercise
    loader.load(context, linkConfig, toJobConfig);

    // verify
    verify(executorMock, times(NUMBER_OF_ROWS)).writeRecord(
        any(Object[].class));
  }

}