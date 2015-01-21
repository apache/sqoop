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
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.Reader;

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

  private JSONUtils() {
    // Instantiation is prohibited
  }
}
