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

import static org.testng.AssertJUnit.assertEquals;

import java.util.ResourceBundle;

import org.apache.sqoop.json.util.ConfigTestUtil;
import org.apache.sqoop.model.MDriver;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

/**
 *
 */
public class TestDriverBean {

  /**
   * Test that by JSON serialization followed by deserialization we will get
   * equal drive config object.
   */
  @Test
  public void testSerialization() {
    MDriver driver = new MDriver(ConfigTestUtil.getDriverConfig(), DriverBean.CURRENT_DRIVER_VERSION);

    // Serialize it to JSON object
    DriverBean bean = new DriverBean(driver, ConfigTestUtil.getResourceBundle());
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = JSONUtils.parse(string);
    DriverBean retrievedBean = new DriverBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(driver, retrievedBean.getDriver());

    ResourceBundle retrievedBundle = retrievedBean.getDriverConfigResourceBundle();
    assertEquals("a", retrievedBundle.getString("a"));
    assertEquals("b", retrievedBundle.getString("b"));
  }

}
