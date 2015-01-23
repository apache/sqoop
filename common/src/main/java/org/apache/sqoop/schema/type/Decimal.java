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
 * Fixed point number with configurable precision and scale.
 *
 * JDBC Types: numeric, decimal
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Decimal extends AbstractNumber {

  /**
   * Numeric precision refers to the maximum number of digits that are present in the number.
   * ie 1234567.89 has a precision of 9
   * Numeric scale refers to the maximum number of decimal places
   * ie 123456.789 has a scale of 3
   * Thus the maximum allowed value for decimal(5,2) is 999.99
   */
  private Integer precision;

  private Integer scale;

  public Decimal(String name, Integer precision, Integer scale) {
    super(name);
    this.precision = precision;
    this.scale = scale;
  }

  public Decimal(String name, Boolean nullable, Integer precision, Integer scale) {
    super(name, nullable);
    this.precision = precision;
    this.scale = scale;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.DECIMAL;
  }

  @Override
  public String toString() {
    return new StringBuilder("Decimal{")
      .append(super.toString())
      .append(",precision=").append(precision)
      .append(",scale=").append(scale)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Decimal)) return false;
    if (!super.equals(o)) return false;

    Decimal decimal = (Decimal) o;

    if (precision != null ? !precision.equals(decimal.precision) : decimal.precision != null)
      return false;
    if (scale != null ? !scale.equals(decimal.scale) : decimal.scale != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (precision != null ? precision.hashCode() : 0);
    result = 31 * result + (scale != null ? scale.hashCode() : 0);
    return result;
  }
}
