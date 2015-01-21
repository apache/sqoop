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

import java.util.List;

/**
 * Config describing all required information to build up an link object
 * NOTE: It extends a config list since {@link MLink} could consist of a related config groups
 *       In future this could be simplified to hold a single list of all configs for the link object
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MLinkConfig extends MConfigList {

  public MLinkConfig(List<MConfig> configs) {
    super(configs, MConfigType.LINK);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Link: ");
    sb.append(super.toString());
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    return super.equals(other);
  }

  @Override
  public MLinkConfig clone(boolean cloneWithValue) {
    MLinkConfig copy = new MLinkConfig(super.clone(cloneWithValue).getConfigs());
    return copy;
  }
}
