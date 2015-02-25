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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the various config types supported by the system.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum MConfigType {

  /** Unknown config type */
  OTHER,

  @Deprecated
  // NOTE: only exists to support the connector data upgrade path
  CONNECTION,

  /** link config type */
  LINK("link"),

  /** Job config type */
  // NOTE: cannot use the constants declared below since it is not declared yet
  // compiler restriction
  JOB("from", "to", "driver");

  private final Set<String> subTypes;

  MConfigType(String... subTypes) {
    Set<String> subT = new HashSet<String>();
    subT.addAll(Arrays.asList(subTypes));
    this.subTypes = Collections.unmodifiableSet(subT);
  }

  public static Set<String> getSubTypes(MConfigType type) {
    return type.subTypes;
  }

  public static final String FROM_SUB_TYPE = "from";
  public static final String TO_SUB_TYPE = "to";
  public static final String DRIVER_SUB_TYPE = "driver";
}
