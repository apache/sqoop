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

package org.apache.sqoop.db.decorator;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.authentication.KerberosAuthenticator;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
public class TestKerberizedConnectionFactoryDecorator {

  private KerberizedConnectionFactoryDecorator kerberizedConnectionFactoryDecorator;

  private KerberosAuthenticator kerberosAuthenticator;

  private JdbcConnectionFactory decoratedFactory;

  private UserGroupInformation testUser;

  private UserGroupInformation capturedCurrentUser;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    decoratedFactory = mock(JdbcConnectionFactory.class);
    kerberosAuthenticator = mock(KerberosAuthenticator.class);
    testUser = UserGroupInformation.createUserForTesting("testUser", new String[]{});
    when(kerberosAuthenticator.authenticate()).thenReturn(testUser);

    kerberizedConnectionFactoryDecorator = new KerberizedConnectionFactoryDecorator(decoratedFactory, kerberosAuthenticator);
  }

  @Test
  public void testCreateConnectionIsInvokedAsAuthenticatedUser() throws Exception {
    // We want to capture the current user when the createConnection() method is invoked on the decorated factory.
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        capturedCurrentUser = UserGroupInformation.getCurrentUser();
        return null;
      }
    }).when(decoratedFactory).createConnection();

    kerberizedConnectionFactoryDecorator.createConnection();

    assertEquals(testUser, capturedCurrentUser);
  }

  @Test
  public void testCreateConnectionReturnsConnectionCreatedByDecoratedFactory() throws Exception {
    Connection expected = mock(Connection.class);
    when(decoratedFactory.createConnection()).thenReturn(expected);

    assertSame(expected, kerberizedConnectionFactoryDecorator.createConnection());
  }

  @Test
  public void testCreateConnectionThrowsTheSameExceptionDecoratedFactoryThrows() throws Exception {
    RuntimeException expected = mock(RuntimeException.class);
    when(decoratedFactory.createConnection()).thenThrow(expected);

    expectedException.expect(equalTo(expected));
    kerberizedConnectionFactoryDecorator.createConnection();
  }

}
