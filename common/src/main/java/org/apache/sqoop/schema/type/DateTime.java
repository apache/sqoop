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
 * Date and time information together.
 *
 * JDBC Types: datetime, timestamp
 */
public class DateTime extends AbstractDateTime {

  /**
   * The column can contain fractions of seconds.
   */
  private Boolean fraction;

  /**
   * The column do have encoded timezone.
   */
  private Boolean timezone;

  public DateTime() {
  }

  public DateTime(String name) {
    super(name);
  }

  public DateTime(Boolean fraction, Boolean timezone) {
    this.fraction = fraction;
    this.timezone = timezone;
  }

  public DateTime(String name, Boolean fraction, Boolean timezone) {
    super(name);
    this.fraction = fraction;
    this.timezone = timezone;
  }

  public DateTime(String name, Boolean nullable, Boolean fraction, Boolean timezone) {
    super(name, nullable);
    this.fraction = fraction;
    this.timezone = timezone;
  }

  public Boolean getFraction() {
    return fraction;
  }

  public DateTime setFraction(Boolean fraction) {
    this.fraction = fraction;
    return this;
  }

  public Boolean getTimezone() {
    return timezone;
  }

  public DateTime setTimezone(Boolean timezone) {
    this.timezone = timezone;
    return this;
  }

  @Override
  public Type getType() {
    return Type.DATE_TIME;
  }

  @Override
  public String toString() {
    return new StringBuilder("Date{")
      .append(super.toString())
      .append(",fraction=").append(fraction)
      .append(",timezone=").append(timezone)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DateTime)) return false;
    if (!super.equals(o)) return false;

    DateTime dateTime = (DateTime) o;

    if (fraction != null ? !fraction.equals(dateTime.fraction) : dateTime.fraction != null)
      return false;
    if (timezone != null ? !timezone.equals(dateTime.timezone) : dateTime.timezone != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (fraction != null ? fraction.hashCode() : 0);
    result = 31 * result + (timezone != null ? timezone.hashCode() : 0);
    return result;
  }
}
