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

package org.apache.sqoop.authentication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.apache.sqoop.testcategories.sqooptest.IntegrationTest;
import org.apache.sqoop.testcategories.KerberizedTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

@Category({KerberizedTest.class, IntegrationTest.class})
public class TestKerberosAuthenticator {

  private static final String KERBEROS_RULE_TEMPLATE = "RULE:[2:$1@$0](.*@%s)s/@%s//";

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdc = new MiniKdcInfrastructureRule();

  private KerberosAuthenticator kerberosAuthenticator;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAuthenticateReturnsCurrentUserIfKerberosIsNotEnabled() throws Exception {
    kerberosAuthenticator = new KerberosAuthenticator(new Configuration(), miniKdc.getTestPrincipal(), miniKdc.getKeytabFilePath());

    assertSame(UserGroupInformation.getCurrentUser(), kerberosAuthenticator.authenticate());
  }

  @Test
  public void testAuthenticateReturnsAUserDifferentThanCurrentUserIfKerberosIsEnabled() throws Exception {
    kerberosAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), miniKdc.getTestPrincipal(), miniKdc.getKeytabFilePath());

    assertNotSame(UserGroupInformation.getCurrentUser(), kerberosAuthenticator.authenticate());
  }

  @Test
  public void testAuthenticateReturnsAKerberosAuthenticatedUserIfKerberosIsEnabled() throws Exception {
    kerberosAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), miniKdc.getTestPrincipal(), miniKdc.getKeytabFilePath());

    UserGroupInformation authenticatedUser = kerberosAuthenticator.authenticate();
    assertEquals(KERBEROS, authenticatedUser.getRealAuthenticationMethod());
  }

  @Test
  public void testAuthenticateReturnsAnAuthenticatedUserWithProperUsernameIfKerberosIsEnabled() throws Exception {
    kerberosAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), miniKdc.getTestPrincipal(), miniKdc.getKeytabFilePath());

    UserGroupInformation authenticatedUser = kerberosAuthenticator.authenticate();
    assertEquals(miniKdc.getTestPrincipal(), authenticatedUser.getUserName());
  }

  @Test
  public void testAuthenticateThrowsIfKerberosIsEnabledAndInvalidKeytabIsProvided() throws Exception {
    String invalidKeytabLocation = "invalid_keytab_location";
    kerberosAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), miniKdc.getTestPrincipal(), invalidKeytabLocation);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Kerberos authentication failed!");
    kerberosAuthenticator.authenticate();
  }

  @Test
  public void testAuthenticateThrowsIfKerberosIsEnabledAndInvalidPrincipalIsProvided() throws Exception {
    String invalidPrincipal = "invalid_principal";
    kerberosAuthenticator = new KerberosAuthenticator(createKerberosConfiguration(), invalidPrincipal, miniKdc.getKeytabFilePath());

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Kerberos authentication failed!");
    kerberosAuthenticator.authenticate();
  }

  private Configuration createKerberosConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    // Adding a rule for the realm used by the MiniKdc since the default kerberos configuration might contain another realm.
    configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTH_TO_LOCAL, buildKerberosRule());
    return configuration;
  }

  private String buildKerberosRule() {
    return String.format(KERBEROS_RULE_TEMPLATE, miniKdc.getRealm(), miniKdc.getRealm());
  }

}
