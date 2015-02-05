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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Constants related to the configs
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfigInputConstants {

  public static final String CONFIG_ID = "id";
  public static final String INPUT_ID = "id";
  public static final String CONFIG_NAME = "name";
  public static final String CONFIG_TYPE = "type";
  public static final String CONFIG_INPUTS = "inputs";
  public static final String CONFIG_INPUT_NAME = "name";
  public static final String CONFIG_INPUT_TYPE = "type";
  public static final String CONFIG_INPUT_SENSITIVE = "sensitive";
  public static final String CONFIG_INPUT_SIZE = "size";
  public static final String CONFIG_INPUT_EDITABLE = "editable";
  public static final String CONFIG_INPUT_OVERRIDES = "overrides";
  public static final String CONFIG_INPUT_VALUE = "value";
  public static final String CONFIG_INPUT_ENUM_VALUES = "values";

  private ConfigInputConstants() {

  }
}
