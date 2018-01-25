package org.apache.sqoop.authentication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class KerberosAuthenticatorTest {

  private static final String EXAMPLE_COM_RULE = "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@EXAMPLE.COM//";

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
    // Adding a rule for EXAMPLE.COM since the default kerberos configuration might contain another realm.
    configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTH_TO_LOCAL, EXAMPLE_COM_RULE);
    return configuration;
  }

}
