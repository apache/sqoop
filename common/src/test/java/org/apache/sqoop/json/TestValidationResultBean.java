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
import org.apache.sqoop.validation.ConfigValidationResult;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 */
public class TestValidationResultBean {

  @Test
  public void testEmptyTransfer() {
    ConfigValidationResult []empty = new ConfigValidationResult[0];

    ConfigValidationResult []retrieved = transfer(empty);
    assertEquals(0, retrieved.length);
  }

  @Test
  public void testOneMessage() {
    ConfigValidationResult []empty = new ConfigValidationResult[] {
      getResultA()
    };

    ConfigValidationResult []retrieved = transfer(empty);
    assertEquals(1, retrieved.length);
    verifyResultA(retrieved[0]);
  }

  @Test
  public void testTwoMessages() {
     ConfigValidationResult []empty = new ConfigValidationResult[] {
      getResultA(),
      getResultA()
    };

    ConfigValidationResult []retrieved = transfer(empty);
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

  public void verifyResultA(ConfigValidationResult result) {
    assertNotNull(result);
    assertEquals(Status.ERROR, result.getStatus());

    Map<String, List<Message>> messages = result.getMessages();
    assertEquals(1, messages.size());

    assertTrue(messages.containsKey("A"));
    List<Message> messagesA = messages.get("A");
    assertNotNull(messagesA);
    assertEquals(2, messagesA.size());

    assertEquals(Status.WARNING, messagesA.get(0).getStatus());
    assertEquals("A", messagesA.get(0).getMessage());

    assertEquals(Status.ERROR, messagesA.get(1).getStatus());
    assertEquals("B", messagesA.get(1).getMessage());
  }

  public ConfigValidationResult getResultA() {
    ConfigValidationResult result = new ConfigValidationResult();
    List<Message> messages = new LinkedList<Message>();
    messages.add(new Message(Status.WARNING, "A"));
    messages.add(new Message(Status.ERROR, "B"));
    result.addMessages("A", messages);
    return result;
  }


  private Long transfer(Long id) {
    ValidationResultBean bean = new ValidationResultBean(new ConfigValidationResult[0]);
    bean.setId(id);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = JSONUtils.parse(string);
    ValidationResultBean retrievedBean = new ValidationResultBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getId();
  }

  private ConfigValidationResult[] transfer(ConfigValidationResult [] results) {
    ValidationResultBean bean = new ValidationResultBean(results);
    JSONObject json = bean.extract(false);

    String string = json.toString();

    JSONObject retrievedJson = JSONUtils.parse(string);
    ValidationResultBean retrievedBean = new ValidationResultBean();
    retrievedBean.restore(retrievedJson);

    return retrievedBean.getValidationResults();
  }
}
