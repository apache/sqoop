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
package org.apache.sqoop.schema.type;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Floating point represented as IEEE norm.
 *
 * JDBC Types: double, float, real
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FloatingPoint extends AbstractNumber {

  /**
  This field will come handy in connector that might require to use the
  size information on the schema object to do additional type mappings in their source
  Read more information : https://issues.apache.org/jira/secure/attachment/12589331/Sqoop2Datatypes.pdf
  */
  private Long byteSize;

  public FloatingPoint(String name, Long byteSize) {
    super(name);
    this.byteSize = byteSize;
  }

  public FloatingPoint(String name, Boolean nullable, Long byteSize) {
    super(name, nullable);
    this.byteSize = byteSize;
  }

  public Long getByteSize() {
    return byteSize;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.FLOATING_POINT;
  }

  @Override
  public String toString() {
    return new StringBuilder("FloatingPoint{")
      .append(super.toString())
      .append(",byteSize=").append(byteSize)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FloatingPoint)) return false;
    if (!super.equals(o)) return false;

    FloatingPoint that = (FloatingPoint) o;

    if (byteSize != null ? !byteSize.equals(that.byteSize) : that.byteSize != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (byteSize != null ? byteSize.hashCode() : 0);
    return result;
  }
}
