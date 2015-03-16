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
package org.apache.sqoop.common;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.Map;

/**
 * Immutable context interface for key value pairs.
 *
 * Useful for configuration objects that are not allowed to change.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ImmutableContext extends Iterable<Map.Entry<String,String>> {

  /**
   * Return string for given key or null by default.
   *
   * @param key Key
   * @return Value for given key or null in case of unknown key
   */
  String getString(String key);

  /**
   * Return string for given key or default value.
   *
   * @param key Key
   * @param defaultValue Default value
   * @return Value for given key or default value in case of unknown key
   */
  String getString(String key, String defaultValue);

  /**
   * Return long for given key or default value.
   *
   * @param key Key
   * @param defaultValue Default value
   * @return Value for given key or default value in case of unknown key
   */
  public long getLong(String key, long defaultValue);

  /**
   * Return int for given key or default value.
   *
   * @param key Key
   * @param defaultValue Default value
   * @return Value for given key or default value in case of unknown key
   */
  public int getInt(String key, int defaultValue);

  /**
   * Return boolean for given key or default value.
   *
   * @param key Key
   * @param defaultValue Default value
   * @return Value for given key or default value in case of unknown key
   */
  public boolean getBoolean(String key, boolean defaultValue);
}
