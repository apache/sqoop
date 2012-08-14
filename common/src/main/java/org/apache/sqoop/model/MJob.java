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

import java.util.List;

/**
 * Metadata for specific job type. Internally Sqoop have two different job
 * metadata for each job type. One for connector and second for framework.
 */
public class MJob extends MPersistableEntity {


  static public enum Type {
    IMPORT,
    EXPORT,
  }

  private final Type type;
  private final List<MForm> forms;

  public MJob(Type type, List<MForm> forms) {
    this.type = type;
    this.forms = forms;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("job type:").append(type.name());
    sb.append(", forms: ").append(forms);
    return sb.toString();
  }

  public Type getType() {
    return type;
  }

  public List<MForm> getForms() {
    return forms;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MJob)) {
      return false;
    }

    MJob mj = (MJob) other;
    return type.equals(mj.type) && forms.equals(mj.forms);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + type.hashCode();
    for(MForm form : forms) {
      result = 31 * result + form.hashCode();
    }

    return result;
  }
}
