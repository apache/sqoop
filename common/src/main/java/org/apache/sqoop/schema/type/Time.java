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
 * Time (hours, minutes, seconds).
 *
 * JDBC Types: time
 */
public class Time extends AbstractDateTime {

  private Boolean fraction;

  public Time() {
  }

  public Time(String name) {
    super(name);
  }

  public Time(Boolean fraction) {
    this.fraction = fraction;
  }

  public Time(String name, Boolean fraction) {
    super(name);
    this.fraction = fraction;
  }

  public Time(String name, Boolean nullable, Boolean fraction) {
    super(name, nullable);
    this.fraction = fraction;
  }

  public Boolean getFraction() {
    return fraction;
  }

  public Time setFraction(Boolean fraction) {
    this.fraction = fraction;
    return this;
  }

  @Override
  public Type getType() {
    return Type.TIME;
  }

  @Override
  public String toString() {
    return new StringBuilder("Time{")
      .append(super.toString())
      .append(",fraction=").append(fraction)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Time)) return false;
    if (!super.equals(o)) return false;

    Time time = (Time) o;

    if (fraction != null ? !fraction.equals(time.fraction) : time.fraction != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (fraction != null ? fraction.hashCode() : 0);
    return result;
  }
}
