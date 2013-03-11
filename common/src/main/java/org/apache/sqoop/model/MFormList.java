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

import org.apache.sqoop.common.SqoopException;

import java.util.List;

/**
 * Arbitrary list of forms.
 */
public class MFormList {

  private final List<MForm> forms;

  public MFormList(List<MForm> forms) {
    this.forms = forms;
  }

  public List<MForm> getForms() {
    return forms;
  }

  public MForm getForm(String formName) {
    for(MForm form: forms) {
      if(formName.equals(form.getName())) {
        return form;
      }
    }

    throw new SqoopException(ModelError.MODEL_010, "Form name: " + formName);
  }

  public MInput getInput(String name) {
    String []parts = name.split("\\.");
    if(parts.length != 2) {
      throw new SqoopException(ModelError.MODEL_009, name);
    }

    return getForm(parts[0]).getInput(name);
  }

  public MStringInput getStringInput(String name) {
    return (MStringInput)getInput(name);
  }

  public MEnumInput getEnumInput(String name) {
    return (MEnumInput)getInput(name);
  }

  public MIntegerInput getIntegerInput(String name) {
    return (MIntegerInput)getInput(name);
  }

  public MMapInput getMapInput(String name) {
    return (MMapInput)getInput(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MFormList)) return false;

    MFormList mFormList = (MFormList) o;

    if (!forms.equals(mFormList.forms)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    for(MForm form : forms) {
      result = 31 * result + form.hashCode();
    }

    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Forms: ");
    for(MForm form : forms) {
      sb.append(form.toString());
    }
    return sb.toString();
  }
}
