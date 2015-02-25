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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.utils.UrlSafeUtils;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class MMapInput extends MInput<Map<String, String>> {

  public MMapInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name, sensitive, editable, overrides);
  }

  @Override
  public String getUrlSafeValueString() {
    Map<String, String> valueMap = getValue();
    if (valueMap == null) {
      return null;
    }
    boolean first = true;
    StringBuilder vsb = new StringBuilder();
    for (Map.Entry<String, String> entry : valueMap.entrySet()) {
      if (first) {
        first = false;
      } else {
        vsb.append("&");
      }
      vsb.append(UrlSafeUtils.urlEncode(entry.getKey())).append("=");
      vsb.append(entry.getValue() != null ? UrlSafeUtils.urlEncode(entry.getValue()): null);
    }
    return vsb.toString();
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if (valueString == null) {
      setValue(null);
    } else {
      Map<String, String> valueMap = new HashMap<String, String>();
      if (valueString.trim().length() > 0) {
        String[] valuePairs = valueString.split("&");
        for (String pair : valuePairs) {
          String[] nameAndVal = pair.split("=");
          if (nameAndVal.length > 0) {
            String name = nameAndVal[0];
            String value = null;
            if (nameAndVal.length > 1) {
              value = nameAndVal[1];
            }

            valueMap.put(name, value);
          }
        }
      }
      setValue(valueMap);
    }
  }

  @Override
  public MInputType getType() {
    return MInputType.MAP;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MMapInput)) {
      return false;
    }

    MMapInput mmi = (MMapInput) other;
    return getName().equals(mmi.getName());
  }

  @Override
  public int hashCode() {
    return 23 + 31 * getName().hashCode();
  }

  @Override
  public boolean isEmpty() {
    return getValue() == null;
  }

  @Override
  public void setEmpty() {
    setValue(null);
  }

  @Override
  public MMapInput clone(boolean cloneWithValue) {
    MMapInput copy = new MMapInput(getName(), isSensitive(), getEditable(), getOverrides());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue && this.getValue() != null) {
      Map<String, String> copyMap = new HashMap<String, String>();
      Set<Map.Entry<String, String>> entry = this.getValue().entrySet();
      for(Map.Entry<String, String> itr : entry) {
        copyMap.put(itr.getKey(), itr.getValue());
      }
      copy.setValue(copyMap);
    }
    return copy;
  }
}
