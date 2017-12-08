package org.apache.sqoop.hive;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS;

public class PasswordAuthenticationConfiguration implements AuthenticationConfiguration {

  private static String TEST_USERNAME;

  private static String TEST_PASSWORD;

  private static final class TestPasswordAuthenticationProvider implements PasswdAuthenticationProvider {

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
      if (!(TEST_USERNAME.equals(user) && TEST_PASSWORD.equals(password))) {
        throw new AuthenticationException("Authentication failed!");
      }
    }
  }

  public PasswordAuthenticationConfiguration(String testUsername, String testPassword) {
    TEST_USERNAME = testUsername;
    TEST_PASSWORD = testPassword;
  }

  @Override
  public Map<String, String> getAuthenticationConfig() {
    Map<String, String> result = new HashMap<>();
    result.put(HIVE_SERVER2_AUTHENTICATION.varname, HiveAuthFactory.AuthTypes.CUSTOM.getAuthName());
    result.put(HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname, TestPasswordAuthenticationProvider.class.getName());

    return result;
  }
}
