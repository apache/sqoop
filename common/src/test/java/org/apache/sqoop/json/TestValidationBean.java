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
package org.apache.sqoop.json;

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidatedElement;
import org.apache.sqoop.validation.Status;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import static org.apache.sqoop.json.TestUtil.getConnection;
import static org.junit.Assert.*;

/**
 *
 */
public class TestValidationBean {

  private static final String CONNECTOR_NAME = "coolest-connector";
  private static final String ERROR_MESSAGE = "Houston, we have a problem!";

  @Test
  public void testSerialization() {
    MConnection connection = getConnection(CONNECTOR_NAME);
    MConnection target = getConnection(CONNECTOR_NAME);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) connection.getConnectorPart().getForms()
      .get(0).getInputs().get(0);
    input.setErrorMessage(ERROR_MESSAGE);

    // Serialize it to JSON object
    ValidationBean bean = new ValidationBean(connection, Status.UNACCEPTABLE);
    JSONObject json = bean.extract();

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    ValidationBean retrievedBean = new ValidationBean(target);
    retrievedBean.restore(retrievedJson);

    // Test that value had been correctly moved
    MStringInput targetInput = (MStringInput) target.getConnectorPart()
      .getForms().get(0).getInputs().get(0);
    assertEquals(MValidatedElement.Severity.ERROR,
      targetInput.getValidationSeverity());
    assertEquals(ERROR_MESSAGE,
      targetInput.getValidationMessage());
  }
}
