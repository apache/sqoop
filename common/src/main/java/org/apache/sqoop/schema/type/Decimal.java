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

/**
 * Fixed point number with configurable precision and scale.
 *
 * JDBC Types: numeric, decimal
 */
public class Decimal extends AbstractNumber {

  /**
   * Number of valid numbers.
   */
  private Long precision;

  /**
   * Number of decimal places.
   */
  private Long scale;

  public Decimal() {
  }

  public Decimal(String name) {
    super(name);
  }

  public Decimal(Long precision, Long scale) {
    this.precision = precision;
    this.scale = scale;
  }

  public Decimal(String name, Long precision, Long scale) {
    super(name);
    this.precision = precision;
    this.scale = scale;
  }

  public Decimal(String name, Boolean nullable, Long precision, Long scale) {
    super(name, nullable);
    this.precision = precision;
    this.scale = scale;
  }

  public Long getPrecision() {
    return precision;
  }

  public Decimal setPrecision(Long precision) {
    this.precision = precision;
    return this;
  }

  public Long getScale() {
    return scale;
  }

  public Decimal setScale(Long scale) {
    this.scale = scale;
    return this;
  }

  @Override
  public Type getType() {
    return Type.DECIMAL;
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
