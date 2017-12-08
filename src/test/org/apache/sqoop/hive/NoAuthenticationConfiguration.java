package org.apache.sqoop.hive;

import java.util.Collections;
import java.util.Map;

public class NoAuthenticationConfiguration implements AuthenticationConfiguration {
  @Override
  public Map<String, String> getAuthenticationConfig() {
    return Collections.emptyMap();
  }
}
