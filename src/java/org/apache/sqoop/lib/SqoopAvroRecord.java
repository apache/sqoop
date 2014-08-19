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
package org.apache.sqoop.lib;

import org.apache.avro.generic.GenericRecord;
import org.apache.sqoop.avro.AvroUtil;

/**
 * The abstract class extends {@link org.apache.sqoop.lib.SqoopRecord}. It also
 * implements the interface GenericRecord which is a generic instance of an Avro
 * record schema. Fields are accessible by name as well as by index.
 */
public abstract class SqoopAvroRecord extends SqoopRecord implements GenericRecord {

  public abstract boolean getBigDecimalFormatString();

  @Override
  public void put(String key, Object v) {
    getFieldMap().put(key, v);
  }

  @Override
  public Object get(String key) {
    Object o = getFieldMap().get(key);
    return AvroUtil.toAvro(o, getBigDecimalFormatString());
  }

  @Override
  public void put(int i, Object v) {
    put(getFieldNameByIndex(i), v);
  }

  @Override
  public Object get(int i) {
    return get(getFieldNameByIndex(i));
  }

  private String getFieldNameByIndex(int i) {
    return getSchema().getFields().get(i).name();
  }

}
