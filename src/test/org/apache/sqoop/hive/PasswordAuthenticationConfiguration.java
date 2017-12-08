package org.apache.sqoop.hive;

import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS;

public class PasswordAuthenticationConfiguration implements AuthenticationConfiguration {

  public static final class TestPasswordAuthenticationProvider implements PasswdAuthenticationProvider {

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
      throw new AuthenticationException("BOOOOOM!!!");
    }
  }

  public PasswordAuthenticationConfiguration(String testUsername, String testPassword) {

  }

  @Override
  public Map<String, String> getAuthenticationConfig() {
    Map<String, String> result = new HashMap<>();
    result.put(HIVE_SERVER2_AUTHENTICATION.varname, HiveAuthFactory.AuthTypes.CUSTOM.getAuthName());
    result.put(HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname, TestPasswordAuthenticationProvider.class.getName());

    return result;
  }
}
