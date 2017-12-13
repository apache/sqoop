package org.apache.sqoop.hive.minicluster;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.sqoop.infrastructure.kerberos.KerberosConfigurationProvider;

import java.util.HashMap;
import java.util.Map;

public class KerberosAuthenticationConfiguration implements AuthenticationConfiguration {

  private final KerberosConfigurationProvider kerberosConfig;

  public KerberosAuthenticationConfiguration(KerberosConfigurationProvider kerberosConfig) {
    this.kerberosConfig = kerberosConfig;
  }

  @Override
  public Map<String, String> getAuthenticationConfig() {
    Map<String, String> result = new HashMap<>();

    result.put(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname, kerberosConfig.getTestPrincipal());
    result.put(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB.varname, kerberosConfig.getKeytabFilePath());
    result.put(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, HiveAuthFactory.AuthTypes.KERBEROS.toString());
    result.put(YarnConfiguration.RM_PRINCIPAL, kerberosConfig.getTestPrincipal());

    return result;
  }
}
