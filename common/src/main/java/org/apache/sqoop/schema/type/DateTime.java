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
 * Date and time information together.
 *
 * JDBC Types: datetime, timestamp
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DateTime extends AbstractDateTime {

  /**
   * The column can contain fractions of seconds (a.k.a nanos)
   */
  private Boolean hasFraction;

  /**
   * The column do have encoded timezone.
   */
  private Boolean hasTimezone;

  public DateTime(String name, Boolean hasFraction, Boolean hasTimezone) {
    super(name);
    this.hasFraction = hasFraction;
    this.hasTimezone = hasTimezone;
  }

  public DateTime(String name, Boolean nullable, Boolean hasFraction, Boolean hasTimezone) {
    super(name, nullable);
    this.hasFraction = hasFraction;
    this.hasTimezone = hasTimezone;
  }

  public Boolean hasFraction() {
    return hasFraction;
  }

  public DateTime setFraction(Boolean fraction) {
    this.hasFraction = fraction;
    return this;
  }

  public Boolean hasTimezone() {
    return hasTimezone;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.DATE_TIME;
  }

  @Override
  public String toString() {
    return new StringBuilder("Date{")
      .append(super.toString())
      .append(",hasFraction=").append(hasFraction)
      .append(",hasTimezone=").append(hasTimezone)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DateTime)) return false;
    if (!super.equals(o)) return false;

    DateTime dateTime = (DateTime) o;

    if (hasFraction != null ? !hasFraction.equals(dateTime.hasFraction) : dateTime.hasFraction != null)
      return false;
    if (hasTimezone != null ? !hasTimezone.equals(dateTime.hasTimezone) : dateTime.hasTimezone != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (hasFraction != null ? hasFraction.hashCode() : 0);
    result = 31 * result + (hasTimezone != null ? hasTimezone.hashCode() : 0);
    return result;
  }
}
