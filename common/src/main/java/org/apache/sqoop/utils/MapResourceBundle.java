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
package org.apache.sqoop.utils;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Wrapper class to hold the resource bundle key-value pairs in a collections map object
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapResourceBundle extends ResourceBundle {

  private Map<String, Object> bundle;

  public MapResourceBundle(Map<String, Object> bundle) {
    this.bundle = bundle;
  }

  @Override
  protected Object handleGetObject(String key) {
    if(!bundle.containsKey(key)) {
      return null;
    }

    return bundle.get(key);
  }

  @Override
  public Enumeration<String> getKeys() {
    return Collections.enumeration(bundle.keySet());
  }
}
