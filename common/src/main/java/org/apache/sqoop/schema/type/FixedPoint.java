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
 * Basic non-floating number.
 *
 * JDBC Types: int, long, bigint, smallint
 */
public class FixedPoint extends AbstractNumber {

  /**
   This field will come handy in connectors that might require to use the
   size information to do additional type mappings in their data source
   For example in Hive.
   Default: bigint
   if size < 1 then tinyint
   if size < 2 then smallint
   if size < 4 then int
   Read more: https://issues.apache.org/jira/secure/attachment/12589331/Sqoop2Datatypes.pdf
   */
  private Long byteSize;

  private Boolean unsigned;

  public FixedPoint(String name) {
    super(name);
  }

  public FixedPoint(String name, Long byteSize, Boolean unsigned) {
    super(name);
    this.byteSize = byteSize;
    this.unsigned = unsigned;
  }

  public FixedPoint(String name, Boolean nullable, Long byteSize, Boolean unsigned) {
    super(name, nullable);
    this.byteSize = byteSize;
    this.unsigned = unsigned;
  }

  public Long getByteSize() {
    return byteSize;
  }

  public FixedPoint setByteSize(Long byteSize) {
    this.byteSize = byteSize;
    return this;
  }

  public Boolean getUnsigned() {
    return unsigned;
  }

  public FixedPoint setUnsigned(Boolean unsigned) {
    this.unsigned = unsigned;
    return this;
  }

  @Override
  public ColumnType getType() {
    return ColumnType.FIXED_POINT;
  }

  @Override
  public String toString() {
    return new StringBuilder("FixedPoint{")
      .append(super.toString())
      .append(",byteSize=").append(byteSize)
      .append(",unsigned=").append(unsigned)
      .append("}")
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FixedPoint)) return false;
    if (!super.equals(o)) return false;

    FixedPoint that = (FixedPoint) o;

    if (byteSize != null ? !byteSize.equals(that.byteSize) : that.byteSize != null)
      return false;
    if (unsigned != null ? !unsigned.equals(that.unsigned) : that.unsigned != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (byteSize != null ? byteSize.hashCode() : 0);
    result = 31 * result + (unsigned != null ? unsigned.hashCode() : 0);
    return result;
  }
}
