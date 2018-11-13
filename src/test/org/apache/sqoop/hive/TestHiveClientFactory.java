/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class TestHiveClientFactory {

  private static final String TEST_HS2_URL = "jdbc:hive2://myhost:10000/default";

  private static final String TEST_TABLE_NAME = "testTableName";

  private static final String TEST_HIVE_TABLE_NAME = "testHiveTableName";

  private HiveClientFactory hiveClientFactory;

  private ConnManager connectionManager;

  private SqoopOptions sqoopOptions;

  private Configuration configuration;

  private JdbcConnectionFactory jdbcConnectionFactory;

  private HiveServer2ConnectionFactoryInitializer connectionFactoryInitializer;

  private SoftAssertions softly;

  @Before
  public void before() {
    connectionFactoryInitializer = mock(HiveServer2ConnectionFactoryInitializer.class);
    hiveClientFactory = new HiveClientFactory(connectionFactoryInitializer);
    softly = new SoftAssertions();

    connectionManager = mock(ConnManager.class);
    sqoopOptions = mock(SqoopOptions.class);
    configuration = mock(Configuration.class);
    jdbcConnectionFactory = mock(JdbcConnectionFactory.class);

    when(sqoopOptions.getConf()).thenReturn(configuration);
    when(sqoopOptions.getTableName()).thenReturn(TEST_TABLE_NAME);
    when(sqoopOptions.getHiveTableName()).thenReturn(TEST_HIVE_TABLE_NAME);
  }

  @Test
  public void testCreateHiveClientCreatesHiveImportWhenHs2UrlIsNotProvided() throws Exception {
    HiveClient hiveClient = hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    assertThat(hiveClient, instanceOf(HiveImport.class));
  }

  @Test
  public void testCreateHiveClientInitializesHiveImportProperly() throws Exception {
    HiveImport hiveImport = (HiveImport) hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);

    softly.assertThat(hiveImport.getOptions()).isSameAs(sqoopOptions);
    softly.assertThat(hiveImport.getConnManager()).isSameAs(connectionManager);
    softly.assertThat(hiveImport.getConfiguration()).isSameAs(configuration);
    softly.assertThat(hiveImport.isGenerateOnly()).isFalse();
    softly.assertAll();
  }

  @Test
  public void testCreateHiveClientCreatesHiveServer2ClientWhenHs2UrlIsProvided() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    HiveClient hiveClient = hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    assertThat(hiveClient, instanceOf(HiveServer2Client.class));
  }

  @Test
  public void testCreateHiveClientInitializesHiveServer2ClientProperly() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    when(connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions)).thenReturn(jdbcConnectionFactory);

    HiveServer2Client hs2Client = (HiveServer2Client) hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);

    softly.assertThat(hs2Client.getSqoopOptions()).isSameAs(sqoopOptions);
    softly.assertThat(hs2Client.getHs2ConnectionFactory()).isSameAs(jdbcConnectionFactory);
    softly.assertThat(hs2Client.getTableDefWriter().getOptions()).isSameAs(sqoopOptions);
    softly.assertThat(hs2Client.getTableDefWriter().getConnManager()).isSameAs(connectionManager);
    softly.assertThat(hs2Client.getTableDefWriter().getInputTableName()).isEqualTo(TEST_TABLE_NAME);
    softly.assertThat(hs2Client.getTableDefWriter().getOutputTableName()).isEqualTo(TEST_HIVE_TABLE_NAME);
    softly.assertThat(hs2Client.getTableDefWriter().getConfiguration()).isSameAs(configuration);
    softly.assertThat(hs2Client.getTableDefWriter().isCommentsEnabled()).isFalse();

    softly.assertAll();
  }

}