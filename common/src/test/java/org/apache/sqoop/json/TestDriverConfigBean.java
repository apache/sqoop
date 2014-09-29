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

import org.apache.sqoop.model.MDriverConfig;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.ResourceBundle;

import static org.apache.sqoop.json.TestUtil.*;
import static org.junit.Assert.*;

/**
 *
 */
public class TestDriverConfigBean {

  /**
   * Test that by JSON serialization followed by deserialization we will get
   * equal framework object.
   */
  @Test
  public void testSerialization() {
    MDriverConfig driverConfig = getDriverConfig();

    // Serialize it to JSON object
    DriverConfigBean bean = new DriverConfigBean(driverConfig, getResourceBundle());
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    DriverConfigBean retrievedBean = new DriverConfigBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(driverConfig, retrievedBean.getDriverConfig());

    ResourceBundle retrievedBundle = retrievedBean.getResourceBundle();
    assertEquals("a", retrievedBundle.getString("a"));
    assertEquals("b", retrievedBundle.getString("b"));
  }

}
