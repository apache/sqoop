/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 */
public class TestGenericJdbcConnector {

  @Test
  public void testBundleForLink() {
    GenericJdbcConnector connector = new GenericJdbcConnector();
    verifyBundleForConfigClass(connector.getBundle(Locale.getDefault()), connector.getLinkConfigurationClass());
  }

  @Test
  void testBundleForJobToDirection() {
    GenericJdbcConnector connector = new GenericJdbcConnector();
    verifyBundleForConfigClass(connector.getBundle(Locale.getDefault()), connector.getJobConfigurationClass(Direction.TO));
  }

  @Test
  void testBundleForJobFromDirection() {
    GenericJdbcConnector connector = new GenericJdbcConnector();
    verifyBundleForConfigClass(connector.getBundle(Locale.getDefault()), connector.getJobConfigurationClass(Direction.FROM));
  }

  void verifyBundleForConfigClass(ResourceBundle bundle, Class klass) {
    assertNotNull(bundle);
    assertNotNull(klass);

    List<MConfig> configs = ConfigUtils.toConfigs(klass);

    for(MConfig config : configs) {
      assertNotNull(config.getHelpKey());
      assertNotNull(config.getLabelKey());

      assertTrue(bundle.containsKey(config.getHelpKey()), "Can't find help for " + config.getName());
      assertTrue(bundle.containsKey(config.getLabelKey()), "Can't find label for " + config.getName());

      for(MInput input : config.getInputs()) {
        assertNotNull(input.getHelpKey());
        assertNotNull(input.getLabelKey());

        assertTrue(bundle.containsKey(input.getHelpKey()), "Can't find help for " + input.getName());
        assertTrue(bundle.containsKey(input.getLabelKey()), "Can't find label for " + input.getName());
      }
    }
  }
}
