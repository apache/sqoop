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

import java.util.LinkedList;
import java.util.List;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.utils.UrlSafeUtils;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MListInput extends MInput<List<String>> {

  public MListInput(String name, boolean sensitive, InputEditable editable, String overrides) {
    super(name, sensitive, editable, overrides);
  }

  @Override
  public String getUrlSafeValueString() {
    List<String> valueList = getValue();
    if (valueList == null) {
      return null;
    }
    boolean first = true;
    StringBuilder vsb = new StringBuilder();
    for (String element : valueList) {
      if (first) {
        first = false;
      } else {
        vsb.append("&");
      }
      vsb.append(UrlSafeUtils.urlEncode(element));
    }
    return vsb.toString();
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if (valueString == null) {
      setValue(null);
    } else {
      List<String> valueList = new LinkedList<String>();
      if (valueString.trim().length() > 0) {
        for (String element : valueString.split("&")) {
          valueList.add(UrlSafeUtils.urlDecode(element));
        }
      }
      setValue(valueList);
    }
  }

  @Override
  public MInputType getType() {
    return MInputType.LIST;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MListInput)) {
      return false;
    }

    MListInput mli = (MListInput) other;
    return getName().equals(mli.getName());
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
  public Object clone(boolean cloneWithValue) {
    MListInput copy = new MListInput(getName(), isSensitive(), getEditable(), getOverrides());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue && this.getValue() != null) {
      List<String> copyList = new LinkedList<String>();
      for(String element : this.getValue()) {
        copyList.add(element);
      }
      copy.setValue(copyList);
    }
    return copy;
  }
}
