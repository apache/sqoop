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
package org.apache.sqoop.connector.kafka;

import kafka.message.MessageAndMetadata;
import org.apache.sqoop.connector.kafka.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.kafka.configuration.LinkConfiguration;
import org.apache.sqoop.common.test.kafka.TestUtil;
import org.apache.sqoop.etl.io.DataReader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.testng.annotations.AfterClass;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Test(groups="slow")
public class TestKafkaLoader {

  private static TestUtil testUtil = TestUtil.getInstance();
  private static final int NUMBER_OF_ROWS = 1000;
  private static KafkaLoader loader;
  private static String TOPIC = "mytopic";

  @BeforeClass(alwaysRun = true)
  public static void setup() throws IOException {
    testUtil.prepare();
    List<String> topics = new ArrayList<String>(1);
    topics.add(TOPIC);
    testUtil.initTopicList(topics);
    loader = new KafkaLoader();
  }

  @AfterClass(alwaysRun = true)
  public static void tearDown() throws IOException {
    testUtil.tearDown();
  }

  @Test
  public void testLoader() throws Exception {
    LoaderContext context = new LoaderContext(null, new DataReader() {
      private long index = 0L;

      @Override
      public Object[] readArrayRecord() {
        return null;
      }

      @Override
      public String readTextRecord() {
        if (index++ < NUMBER_OF_ROWS) {
          return index + "," + (double)index + ",'" + index + "'";
        } else {
          return null;
        }
      }

      @Override
      public Object readContent() {
        return null;
      }
    }, null);
    LinkConfiguration linkConf = new LinkConfiguration();
    ToJobConfiguration jobConf = new ToJobConfiguration();
    linkConf.linkConfig.brokerList = testUtil.getKafkaServerUrl();
    linkConf.linkConfig.zookeeperConnect = testUtil.getZkUrl();
    jobConf.toJobConfig.topic = TOPIC;

    loader.load(context, linkConf, jobConf);

    for(int i=1;i<=NUMBER_OF_ROWS;i++) {
      MessageAndMetadata<byte[],byte[]> fetchedMsg =
              testUtil.getNextMessageFromConsumer(TOPIC);
      Assert.assertEquals(i + "," + (double) i + "," + "'" + i + "'",
              new String((byte[]) fetchedMsg.message(), "UTF-8"));
    }
  }

}
