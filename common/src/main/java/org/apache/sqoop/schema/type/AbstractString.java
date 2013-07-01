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
 * Any type that is encoding character (or byte) array.
 */
public abstract class AbstractString extends Column {

  private Long size;

  protected AbstractString() {
  }

  protected AbstractString(String name) {
    super(name);
  }

  protected AbstractString(String name, Long size) {
    super(name);
    this.size = size;
  }

  protected AbstractString(String name, Boolean nullable) {
    super(name, nullable);
  }

  protected AbstractString(String name, Boolean nullable, Long size) {
    super(name, nullable);
    this.size = size;
  }

  public Long getSize() {
    return size;
  }

  public AbstractString setSize(Long size) {
    this.size = size;
    return this;
  }

  @Override
  public String toString() {
    return new StringBuilder(super.toString())
      .append(",size=").append(size)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractString)) return false;
    if (!super.equals(o)) return false;

    AbstractString that = (AbstractString) o;

    if (size != null ? !size.equals(that.size) : that.size != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (size != null ? size.hashCode() : 0);
    return result;
  }
}
