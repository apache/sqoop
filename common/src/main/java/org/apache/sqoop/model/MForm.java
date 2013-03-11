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
 * Represents a group of inputs that are processed together. This allows the
 * input gathering process to be broken down into multiple steps that can be
 * then paged through by the user interface.
 */
public final class MForm extends MValidatedElement {

  private final List<MInput<?>> inputs;

  public MForm(String name, List<MInput<?>> inputs) {
    super(name);

    this.inputs = inputs;
  }

  public List<MInput<?>> getInputs() {
    return inputs;
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

  public MMapInput getMapInput(String inputName) {
    return (MMapInput)getInput(inputName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("form-").append(getName());
    sb.append(":").append(getPersistenceId()).append(":").append(inputs);

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MForm)) {
      return false;
    }

    MForm mf = (MForm) other;
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
}
