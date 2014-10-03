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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.validation.ConfigValidator;
import org.apache.sqoop.validation.Status;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

/**
 *
 */
public class TestValidationBean {

  @Test
  public void testJobValidationBeanSerialization() {
    // Serialize it to JSON object
    JobValidationBean bean = new JobValidationBean(
      getValidation(Status.FINE),
      getValidation(Status.UNACCEPTABLE),
      getValidation(Status.FINE)
    );
    JSONObject json = bean.extract(false);

    // "Move" it across network in text config
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    JobValidationBean retrievedBean = new JobValidationBean();
    retrievedBean.restore(retrievedJson);

    assertNull(retrievedBean.getId());

    ConfigValidator.ConfigInput fa = new ConfigValidator.ConfigInput("c", "i");
    ConfigValidator.ConfigInput fb = new ConfigValidator.ConfigInput("c2", "i2");

    ConfigValidator fromConnector = retrievedBean.getConnectorValidation(Direction.FROM);
    assertEquals(Status.FINE, fromConnector.getStatus());
    assertEquals(2, fromConnector.getMessages().size());
    assertTrue(fromConnector.getMessages().containsKey(fa));
    assertEquals(new ConfigValidator.Message(Status.FINE, "d"),
        fromConnector.getMessages().get(fa));

    ConfigValidator toConnector = retrievedBean.getConnectorValidation(Direction.TO);
    assertEquals(Status.FINE, toConnector.getStatus());
    assertEquals(2, toConnector.getMessages().size());
    assertTrue(toConnector.getMessages().containsKey(fa));
    assertEquals(new ConfigValidator.Message(Status.FINE, "d"),
        toConnector.getMessages().get(fa));

    ConfigValidator framework = retrievedBean.getFrameworkValidation();
    assertEquals(Status.UNACCEPTABLE, framework.getStatus());
    assertEquals(2, framework.getMessages().size());
    assertTrue(framework.getMessages().containsKey(fb));
    assertEquals(new ConfigValidator.Message(Status.UNACCEPTABLE, "c"),
      framework.getMessages().get(fb));
  }

  @Test
  public void testJobValidationBeanId() {
    // Serialize it to JSON object
    JobValidationBean jobValidatioBean = new JobValidationBean(
        getValidation(Status.FINE),
        getValidation(Status.FINE),
        getValidation(Status.FINE)
    );
    jobValidatioBean.setId((long) 10);
    JSONObject json = jobValidatioBean.extract(false);

    // "Move" it across network in text config
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    JobValidationBean retrievedBean = new JobValidationBean();
    retrievedBean.restore(retrievedJson);

    assertEquals((Long)(long) 10, retrievedBean.getId());
  }

  @Test
  public void testLinkValidationBeanSerialization() {
    // Serialize it to JSON object
    LinkValidationBean bean = new LinkValidationBean(
        getValidation(Status.FINE));
    JSONObject json = bean.extract(false);

    // "Move" it across network in text config
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    LinkValidationBean retrievedBean = new LinkValidationBean();
    retrievedBean.restore(retrievedJson);

    assertNull(retrievedBean.getId());

    ConfigValidator.ConfigInput ca = new ConfigValidator.ConfigInput("c", "i");
    ConfigValidator connector = retrievedBean.getLinkConfigValidator();
    assertEquals(Status.FINE, connector.getStatus());
    assertEquals(2, connector.getMessages().size());
    assertTrue(connector.getMessages().containsKey(ca));
    assertEquals(new ConfigValidator.Message(Status.FINE, "d"),
        connector.getMessages().get(ca));
  }

  @Test
  public void testLinkValidationBeanId() {
    // Serialize it to JSON object
    LinkValidationBean bean = new LinkValidationBean(
        getValidation(Status.FINE)
    );
    bean.setId((long) 10);
    JSONObject json = bean.extract(false);

    // "Move" it across network in text config
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    LinkValidationBean retrievedBean = new LinkValidationBean();
    retrievedBean.restore(retrievedJson);

    assertEquals((Long)(long) 10, retrievedBean.getId());
  }

  public ConfigValidator getValidation(Status status) {
    Map<ConfigValidator.ConfigInput, ConfigValidator.Message> messages = new HashMap<ConfigValidator.ConfigInput, ConfigValidator.Message>();

    messages.put(new ConfigValidator.ConfigInput("c", "i"), new ConfigValidator.Message(status, "d"));
    messages.put(new ConfigValidator.ConfigInput("c2", "i2"), new ConfigValidator.Message(status, "c"));

    return new ConfigValidator(status, messages);
  }
}
