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

import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ValidationResult;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 */
public class TestValidationResultBean {

  @Test
  public void testEmptyTransfer() {
    ValidationResult []empty = new ValidationResult[0];

    ValidationResult []retrieved = transfer(empty);
    assertEquals(0, retrieved.length);
  }

  @Test
  public void testOneMessage() {
    ValidationResult []empty = new ValidationResult[] {
      getResultA()
    };

    ValidationResult []retrieved = transfer(empty);
    assertEquals(1, retrieved.length);
    verifyResultA(retrieved[0]);
  }

  @Test
  public void testTwoMessages() {
     ValidationResult []empty = new ValidationResult[] {
      getResultA(),
      getResultA()
    };

    ValidationResult []retrieved = transfer(empty);
    assertEquals(2, retrieved.length);

    verifyResultA(retrieved[0]);
    verifyResultA(retrieved[1]);
  }

  @Test
  public void testId() {
    long id = transfer(10L);
    assertEquals(10L, id);

    Long idNull = transfer((Long)null);
    assertNull(idNull);
  }

  public void verifyResultA(ValidationResult result) {
    assertNotNull(result);
    assertEquals(Status.UNACCEPTABLE, result.getStatus());

    Map<String, List<Message>> messages = result.getMessages();
    assertEquals(1, messages.size());

    assertTrue(messages.containsKey("A"));
    List<Message> messagesA = messages.get("A");
    assertNotNull(messagesA);
    assertEquals(2, messagesA.size());

    assertEquals(Status.ACCEPTABLE, messagesA.get(0).getStatus());
    assertEquals("A", messagesA.get(0).getMessage());

    assertEquals(Status.UNACCEPTABLE, messagesA.get(1).getStatus());
    assertEquals("B", messagesA.get(1).getMessage());
  }

  public ValidationResult getResultA() {
    ValidationResult result = new ValidationResult();
    List<Message> messages = new LinkedList<Message>();
    messages.add(new Message(Status.ACCEPTABLE, "A"));
    messages.add(new Message(Status.UNACCEPTABLE, "B"));
    result.addMessages("A", messages);
    return result;
  }


  private Long transfer(Long id) {
    ValidationResultBean bean = new ValidationResultBean(new ValidationResult[0]);
    bean.setId(id);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    ValidationResultBean retrievedBean = new ValidationResultBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getId();
  }

  private ValidationResult[] transfer(ValidationResult [] results) {
    ValidationResultBean bean = new ValidationResultBean(results);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    ValidationResultBean retrievedBean = new ValidationResultBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getValidationResults();
  }
}
