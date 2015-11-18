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

import org.apache.sqoop.common.SqoopException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Date;

import static org.testng.Assert.assertEquals;

/**
 */
public class TestJSONUtils {

  @Test
  public void testString() {
    JSONObject object = JSONUtils.parse("{\"id\":3}");
    assertEquals((long)3, object.get("id"));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testStringInvalid() {
    JSONUtils.parse("{");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testStringNull() {
    JSONUtils.parse((String)null);
  }

  @Test
  public void testReader() {
    JSONObject object = JSONUtils.parse(new StringReader("{\"id\":3}"));
    assertEquals((long)3, object.get("id"));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testReaderInvalid() {
    JSONUtils.parse(new StringReader("{"));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testReaderNull() {
    JSONUtils.parse((Reader)null);
  }

  @Test
  public void testGetType() {
    JSONObject object = JSONUtils.parse("{\"id\":3}");
    assertEquals(new Long(3), JSONUtils.getType(object, "id", Long.class));
  }

  @Test
  public void testGetTypeNull() {
    JSONObject object = JSONUtils.parse("{\"id\": null}");
    assertEquals(null, JSONUtils.getType(object, "id", String.class));
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testGetTypeNonExistingKey() {
    JSONObject object = JSONUtils.parse("{\"id\":3}");
    JSONUtils.getType(object, "non-existing", Long.class);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testGetTypeIncorrectType() {
    JSONObject object = JSONUtils.parse("{\"id\":3}");
    JSONUtils.getType(object, "id", String.class);
  }

  @Test
  public void testGetLong() {
    JSONObject object = JSONUtils.parse("{\"id\":3}");
    assertEquals(new Long(3), JSONUtils.getLong(object, "id"));
  }

  @Test
  public void testGetJSONObject() {
    JSONObject object = JSONUtils.parse("{\"id\": {}}");
    assertEquals(new JSONObject(), JSONUtils.getJSONObject(object, "id"));
  }

  @Test
  public void testGetJSONArray() {
    JSONObject object = JSONUtils.parse("{\"id\": []}");
    assertEquals(new JSONArray(), JSONUtils.getJSONArray(object, "id"));
  }

  @Test
  public void testGetString() {
    JSONObject object = JSONUtils.parse("{\"id\": \"sqoop-is-awesome\"}");
    assertEquals("sqoop-is-awesome", JSONUtils.getString(object, "id"));
  }

  @Test
  public void testGetBoolean() {
    JSONObject object = JSONUtils.parse("{\"id\": true}");
    assertEquals(Boolean.TRUE, JSONUtils.getBoolean(object, "id"));
  }

  @Test
  public void testGetDouble() {
    JSONObject object = JSONUtils.parse("{\"id\": 0.1}");
    assertEquals(0.1, JSONUtils.getDouble(object, "id"));
  }

  @Test
  public void testGetDate() {
    JSONObject object = JSONUtils.parse("{\"id\": 1447628346000}");
    assertEquals(new Date(1447628346000L), JSONUtils.getDate(object, "id"));
  }

  @Test
  public void testGetDateNull() {
    JSONObject object = JSONUtils.parse("{\"id\": null}");
    assertEquals(null, JSONUtils.getDate(object, "id"));
  }
}
