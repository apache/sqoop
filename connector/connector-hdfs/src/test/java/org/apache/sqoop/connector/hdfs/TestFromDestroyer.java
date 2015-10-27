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
package org.apache.sqoop.connector.hdfs;

import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.testng.annotations.Test;
import org.joda.time.DateTime;

import static org.testng.AssertJUnit.assertEquals;

public class TestFromDestroyer {

  Destroyer<LinkConfiguration, FromJobConfiguration> destroyer;
  LinkConfiguration linkConfig;
  FromJobConfiguration jobConfig;
  MutableContext context;
  String user;

  public TestFromDestroyer() {
    linkConfig = new LinkConfiguration();
    jobConfig = new FromJobConfiguration();
    context = new MutableMapContext();
    destroyer = new HdfsFromDestroyer();
    user = "test_user";
  }

  @Test
  public void testUpdateConfiguration() {
    DateTime dt = new DateTime();
    context.setLong(HdfsConstants.MAX_IMPORT_DATE, dt.getMillis());
    destroyer.updateConfiguration(new DestroyerContext(context, true, null, user), linkConfig, jobConfig);
    assertEquals(jobConfig.incremental.lastImportedDate, dt);
  }
}
