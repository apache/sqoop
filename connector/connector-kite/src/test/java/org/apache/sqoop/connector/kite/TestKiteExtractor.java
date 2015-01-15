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

import org.apache.sqoop.connector.kite.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.kite.configuration.LinkConfiguration;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Text;
import org.mockito.stubbing.OngoingStubbing;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestKiteExtractor {

  private KiteExtractor extractor;

  @org.mockito.Mock
  private KiteDatasetExecutor executorMock;

  @org.mockito.Mock
  private DataWriter writerMock = new DataWriter() {
    @Override
    public void writeArrayRecord(Object[] array) {
    }

    @Override
    public void writeStringRecord(String text) {
    }

    @Override
    public void writeRecord(Object obj) {
    }
  };

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    initMocks(this);

    extractor = new KiteExtractor() {
      @Override
      protected KiteDatasetExecutor getExecutor(String uri) {
        return executorMock;
      }
    };
  }

  @Test
  public void testExtractor() throws Exception {
    // setup
    Schema schema = new Schema("testExtractor");
    schema.addColumn(new Text("TextCol"));
    ExtractorContext context = new ExtractorContext(null, writerMock, schema);
    LinkConfiguration linkConfig = new LinkConfiguration();
    FromJobConfiguration jobConfig = new FromJobConfiguration();
    KiteDatasetPartition partition = new KiteDatasetPartition();
    partition.setUri("dataset:hdfs:/path/to/dataset");
    OngoingStubbing<Object[]> readRecordMethodStub = when(executorMock.readRecord());
    final int NUMBER_OF_ROWS = 1000;
    for (int i = 0; i < NUMBER_OF_ROWS; i++) {
      // TODO: SQOOP-1616 will cover more column data types
      readRecordMethodStub = readRecordMethodStub.thenReturn(new Object[]{});
    }
    readRecordMethodStub.thenReturn(null);

    // exercise
    extractor.extract(context, linkConfig, jobConfig, partition);

    // verify
    verify(writerMock, times(NUMBER_OF_ROWS)).writeArrayRecord(
        any(Object[].class));
  }

}