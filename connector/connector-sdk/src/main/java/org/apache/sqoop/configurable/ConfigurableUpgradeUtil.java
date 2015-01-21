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
package org.apache.sqoop.configurable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ConfigurableUpgradeUtil {

  private static final Logger LOG = Logger.getLogger(ConfigurableUpgradeUtil.class);
  /*
   * For now, there is no real upgrade. So copy all data over,
   * set the validation messages and error messages to be the same as for the
   * inputs in the original one.
   */
  @SuppressWarnings("unchecked")
  public static void doUpgrade(List<MConfig> original, List<MConfig> target) {
    Map<String, MConfig> configMap = new HashMap<String, MConfig>();
    for (MConfig config : original) {
      configMap.put(config.getName(), config);
    }
    for (MConfig config : target) {
      List<MInput<?>> inputs = config.getInputs();
      MConfig originalConfig = configMap.get(config.getName());
      if (originalConfig == null) {
        LOG.warn("Config: '" + config.getName() + "' not present in old " +
            "configurable. So it and its inputs will not be transferred by the upgrader.");
        continue;
      }
      for (MInput input : inputs) {
        try {
          MInput originalInput = originalConfig.getInput(input.getName());
          input.setValue(originalInput.getValue());
        } catch (SqoopException ex) {
          LOG.warn("Input: '" + input.getName() + "' not present in old " +
              "configurable. So it will not be transferred by the upgrader.");
        }
      }
    }
  }
}
