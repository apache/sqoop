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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.db.decorator.KerberizedConnectionFactoryDecorator;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class HiveServer2ConnectionFactoryInitializerTest {

  private static final String TEST_HS2_URL = "jdbc:hive2://myhost:10000/default";

  private static final String TEST_HS2_USER = "testuser";

  private static final String TEST_HS2_KEYTAB = "testkeytab";

  private HiveServer2ConnectionFactoryInitializer connectionFactoryInitializer;

  private SqoopOptions sqoopOptions;

  private Configuration configuration;

  private SoftAssertions softly;

  @Before
  public void before() {
    connectionFactoryInitializer = new HiveServer2ConnectionFactoryInitializer();
    sqoopOptions = mock(SqoopOptions.class);
    configuration = mock(Configuration.class);
    softly = new SoftAssertions();

    when(sqoopOptions.getHs2User()).thenReturn(TEST_HS2_USER);
    when(sqoopOptions.getConf()).thenReturn(configuration);
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithoutKerberosConfiguredReturnsHiveServer2ConnectionFactory() throws Exception {
    JdbcConnectionFactory connectionFactory = connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    assertThat(connectionFactory, instanceOf(HiveServer2ConnectionFactory.class));
  }

  @Test
  public void testCreateJdbcConnectionFactoryInitializesConnectionStringProperly() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    HiveServer2ConnectionFactory connectionFactory = (HiveServer2ConnectionFactory) connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    assertEquals(TEST_HS2_URL, connectionFactory.getConnectionString());
  }

  @Test
  public void testCreateJdbcConnectionFactoryInitializesConnectionUsernameProperly() throws Exception {
    HiveServer2ConnectionFactory connectionFactory = (HiveServer2ConnectionFactory) connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    assertEquals(TEST_HS2_USER, connectionFactory.getUsername());
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithoutHs2UserSpecifiedInitializesConnectionUsernameProperly() throws Exception {
    when(sqoopOptions.getHs2User()).thenReturn(null);
    String expectedUsername = UserGroupInformation.getLoginUser().getUserName();
    HiveServer2ConnectionFactory connectionFactory = (HiveServer2ConnectionFactory) connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    assertEquals(expectedUsername, connectionFactory.getUsername());
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithKerberosConfiguredReturnsKerberizedConnectionFactoryDecorator() throws Exception {
    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);

    JdbcConnectionFactory connectionFactory = connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    assertThat(connectionFactory, instanceOf(KerberizedConnectionFactoryDecorator.class));
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithKerberosConfiguredInitializesDecoratorProperly() throws Exception {
    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);

    KerberizedConnectionFactoryDecorator connectionFactory = (KerberizedConnectionFactoryDecorator) connectionFactoryInitializer.createJdbcConnectionFactory(sqoopOptions);

    softly.assertThat(connectionFactory.getDecorated()).isInstanceOf(HiveServer2ConnectionFactory.class);
    softly.assertThat(connectionFactory.getAuthenticator().getConfiguration()).isSameAs(configuration);
    softly.assertThat(connectionFactory.getAuthenticator().getPrincipal()).isEqualTo(TEST_HS2_USER);
    softly.assertThat(connectionFactory.getAuthenticator().getKeytabLocation()).isEqualTo(TEST_HS2_KEYTAB);

    softly.assertAll();
  }

}