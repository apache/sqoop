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
import org.apache.sqoop.common.SqoopException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a group of inputs that are processed together. This allows the
 * input gathering process to be broken down into multiple steps that can be
 * then paged through by the user interface.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class MConfig extends MValidatedElement implements MClonable {

  private final List<MInput<?>> inputs;
  private Set<String> inputNames = new HashSet<String>();
  private Set<String> userOnlyEditableInputNames = new HashSet<String>();

  public MConfig(String name, List<MInput<?>> inputs) {
    super(name);
    this.inputs = inputs;
    if (inputs != null && inputs.size() > 0) {
      for (MInput<?> input : inputs) {
        inputNames.add(input.getName());
        if (input.getEditable().equals(InputEditable.USER_ONLY)) {
          userOnlyEditableInputNames.add(input.getName());
        }
      }
    }
  }

  public List<MInput<?>> getInputs() {
    return inputs;
  }

  public Set<String> getInputNames() {
    return inputNames;
  }

  public Set<String> getUserOnlyEditableInputNames() {
    return userOnlyEditableInputNames;
  }

  public MInput<?> getInput(String inputName) {
    for(MInput<?> input: inputs) {
      if(inputName.equals(input.getName())) {
        return input;
      }
    }

    throw new SqoopException(ModelError.MODEL_011, "Input name: " + inputName);
  }

  public MStringInput getStringInput(String inputName) {
    return (MStringInput)getInput(inputName);
  }

  public MEnumInput getEnumInput(String inputName) {
    return (MEnumInput)getInput(inputName);
  }

  public MIntegerInput getIntegerInput(String inputName) {
    return (MIntegerInput)getInput(inputName);
  }

  public MLongInput getLongInput(String inputName) {
    return (MLongInput)getInput(inputName);
  }

  public MBooleanInput getBooleanInput(String inputName) {
    return (MBooleanInput)getInput(inputName);
  }

  public MMapInput getMapInput(String inputName) {
    return (MMapInput)getInput(inputName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("config-").append(getName());
    sb.append(":").append(getPersistenceId()).append(":").append(inputs);

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MConfig)) {
      return false;
    }

    MConfig mf = (MConfig) other;
    return getName().equals(mf.getName())
        && inputs.equals(mf.inputs);
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + getName().hashCode();
    for (MInput<?> mi : inputs) {
      result = 31 * result + mi.hashCode();
    }

    return result;
  }

  @Override
  public MConfig clone(boolean cloneWithValue) {
    List<MInput<?>> copyInputs = new ArrayList<MInput<?>>();
    for(MInput<?> itr : this.getInputs()) {
      copyInputs.add((MInput<?>)itr.clone(cloneWithValue));
    }
    MConfig copyConfig = new MConfig(this.getName(), copyInputs);
    return copyConfig;
  }
}
