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
package org.apache.sqoop.json.util;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class TestFormSerialization {

//  @Test
//  public void testAllDataTypes() {
//    // Inserted values
//    Map<String, String> map = new HashMap<String, String>();
//    map.put("A", "B");
//
//    // Fill form with all values
//    MForm form = getForm();
//    form.getStringInput("String").setValue("A");
//    form.getMapInput("Map").setValue(map);
//    form.getIntegerInput("Integer").setValue(1);
//    form.getBooleanInput("Boolean").setValue(true);
//    form.getEnumInput("Enum").setValue("YES");
//
//    // Serialize that into JSON
//    JSONObject jsonObject = FormSerialization.extractForm(form, false);
//    assertNotNull(jsonObject);
//
//    // Exchange the data on string level
//    String serializedJson = jsonObject.toJSONString();
//    JSONObject retrievedJson = (JSONObject) JSONValue.parse(serializedJson);
//
//    // And retrieve back from JSON representation
//    MForm retrieved = FormSerialization.restoreForm(retrievedJson);
//
//    // Verify all expected values
//    assertEquals("A", retrieved.getStringInput("String").getValue());
//    assertEquals(map, retrieved.getMapInput("Map").getValue());
//    assertEquals(1, (int)retrieved.getIntegerInput("Integer").getValue());
//    assertEquals(true, retrieved.getBooleanInput("Boolean").getValue());
//    assertEquals("YES", retrieved.getEnumInput("Enum").getValue());
//  }
//
//  @Test
//  public void testMapDataType() {
//    MForm form = getMapForm();
//
//    // Inserted values
//    Map<String, String> map = new HashMap<String, String>();
//    map.put("A", "B");
//    form.getMapInput("Map").setValue(map);
//
//    // Serialize
//    JSONObject jsonObject = FormSerialization.extractForm(form, false);
//    String serializedJson = jsonObject.toJSONString();
//
//    // Deserialize
//    JSONObject retrievedJson = (JSONObject) JSONValue.parse(serializedJson);
//    MForm retrieved = FormSerialization.restoreForm(retrievedJson);
//    assertEquals(map, retrieved.getMapInput("Map").getValue());
//  }
//
//  @Test(expected=SqoopException.class)
//  public void testMapDataTypeException() {
//    MForm form = getMapForm();
//
//    // Inserted values
//    Map<String, String> map = new HashMap<String, String>();
//    map.put("A", "B");
//    form.getMapInput("Map").setValue(map);
//
//    // Serialize
//    JSONObject jsonObject = FormSerialization.extractForm(form, false);
//    String serializedJson = jsonObject.toJSONString();
//
//    // Replace map value with a fake string to force exception
//    String badSerializedJson = serializedJson.replace("{\"A\":\"B\"}", "\"nonsensical string\"");
//    System.out.println(badSerializedJson);
//    JSONObject retrievedJson = (JSONObject) JSONValue.parse(badSerializedJson);
//    FormSerialization.restoreForm(retrievedJson);
//  }
//
//  protected MForm getMapForm() {
//    List<MInput<?>> inputs;
//    MInput input;
//
//    inputs = new LinkedList<MInput<?>>();
//
//    input = new MMapInput("Map", false);
//    inputs.add(input);
//
//    return new MForm("f", inputs);
//  }
//
//  /**
//   * Return form with all data types.
//   *
//   * @return
//   */
//  protected MForm getForm() {
//    List<MInput<?>> inputs;
//    MInput input;
//
//    inputs = new LinkedList<MInput<?>>();
//
//    input = new MStringInput("String", false, (short)30);
//    inputs.add(input);
//
//    input = new MMapInput("Map", false);
//    inputs.add(input);
//
//    input = new MIntegerInput("Integer", false);
//    inputs.add(input);
//
//    input = new MBooleanInput("Boolean", false);
//    inputs.add(input);
//
//    input = new MEnumInput("Enum", false, new String[] {"YES", "NO"});
//    inputs.add(input);
//
//    return new MForm("f", inputs);
//  }
}
