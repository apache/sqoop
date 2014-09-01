/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.hdfs;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJobForms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsMetadataUpgrader extends MetadataUpgrader {
  private static final Logger LOG =
      Logger.getLogger(HdfsMetadataUpgrader.class);

  /*
   * For now, there is no real upgrade. So copy all data over,
   * set the validation messages and error messages to be the same as for the
   * inputs in the original one.
   */

  @Override
  public void upgrade(MConnectionForms original,
                      MConnectionForms upgradeTarget) {
    doUpgrade(original.getForms(), upgradeTarget.getForms());
  }

  @Override
  public void upgrade(MJobForms original, MJobForms upgradeTarget) {
    doUpgrade(original.getForms(), upgradeTarget.getForms());
  }

  @SuppressWarnings("unchecked")
  private void doUpgrade(List<MForm> original, List<MForm> target) {
    // Easier to find the form in the original forms list if we use a map.
    // Since the constructor of MJobForms takes a list,
    // index is not guaranteed to be the same, so we need to look for
    // equivalence
    Map<String, MForm> formMap = new HashMap<String, MForm>();
    for (MForm form : original) {
      formMap.put(form.getName(), form);
    }
    for (MForm form : target) {
      List<MInput<?>> inputs = form.getInputs();
      MForm originalForm = formMap.get(form.getName());
      if (originalForm == null) {
        LOG.warn("Form: '" + form.getName() + "' not present in old " +
            "connector. So it and its inputs will not be transferred by the upgrader.");
        continue;
      }
      for (MInput input : inputs) {
        try {
          MInput originalInput = originalForm.getInput(input.getName());
          input.setValue(originalInput.getValue());
        } catch (SqoopException ex) {
          LOG.warn("Input: '" + input.getName() + "' not present in old " +
              "connector. So it will not be transferred by the upgrader.");
        }
      }
    }
  }
}
