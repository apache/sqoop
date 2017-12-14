package org.apache.sqoop.hive.minicluster;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

public class NoAuthenticationConfiguration implements AuthenticationConfiguration {
  @Override
  public Map<String, String> getAuthenticationConfig() {
    return Collections.emptyMap();
  }

  @Override
  public String getUrlParams() {
    return StringUtils.EMPTY;
  }
}
