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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.util.SerializationError;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;

/**
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JSONUtils {

  /**
   * Parse given string as JSON and return corresponding JSONObject.
   *
   * This method will throw SqoopException on any parsing error.
   *
   * @param input JSON encoded String
   * @return
   */
  public static JSONObject parse(String input) {
    try {
      return (JSONObject) JSONValue.parseWithException(input);
    } catch (ParseException e) {
      throw new SqoopException(SerializationError.SERIALIZATION_002, e);
    }
  }

  /**
   * Parse given reader as JSON and return corresponding JSONObject.
   *
   * This method will throw SqoopException on any parsing error.
   *
   * @param reader
   * @return
   */
  public static JSONObject parse(Reader reader) {
    try {
      return (JSONObject) JSONValue.parseWithException(reader);
    } catch (ParseException e) {
      throw new SqoopException(SerializationError.SERIALIZATION_002, e);
    } catch (IOException e) {
      throw new SqoopException(SerializationError.SERIALIZATION_002, e);
    }
  }

  /**
   * Retrieve safely given key from JSONObject and ensure that it's of given class.
   */
  public static <T> T getType(JSONObject jsonObject, String key, Class<T> targetClass) {
    if(!jsonObject.containsKey(key)) {
      throw new SqoopException(SerializationError.SERIALIZATION_003, "Key: " + key);
    }
    Object ret = jsonObject.get(key);

    if(ret == null) {
      return null;
    }

    if( !(targetClass.isInstance(ret))) {
      throw new SqoopException(SerializationError.SERIALIZATION_004, "Found " + ret.getClass().getName() + " instead of " + targetClass.getName() + " for key: " + key);
    }

    return (T)ret;
  }

  public static Long getLong(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, Long.class);
  }
  public static JSONObject getJSONObject(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, JSONObject.class);
  }
  public static JSONArray getJSONArray(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, JSONArray.class);
  }
  public static String getString(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, String.class);
  }
  public static Boolean getBoolean(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, Boolean.class);
  }
  public static Double getDouble(JSONObject jsonObject, String key) {
    return getType(jsonObject, key, Double.class);
  }
  public static Date getDate(JSONObject jsonObject, String key) {
    Long epoch = getType(jsonObject, key, Long.class);
    if(epoch == null) {
      return null;
    }

    return new Date(epoch);
  }

  private JSONUtils() {
    // Instantiation is prohibited
  }
}
