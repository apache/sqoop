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
package org.apache.sqoop.manager.sqlserver;

import java.util.HashMap;
import java.util.Map;
/**
 * Utilities wrapper class to store properties for
 * a value of a specific data type used for test
 * The properties are enumerated in the KEY_STRING
 */
public class MSSQLTestData implements Comparable {

  MSSQLTestData(String datatypename) {
    this.datatype = datatypename;
    this.data = new HashMap();
  }

  private MSSQLTestData() {

  }

  private String datatype;

  public String getDatatype() {
    return datatype;
  }

  public void setDatatype(String datatype1) {
    this.datatype = datatype1;
  }

  //SCALE: scale of the data
  //PREC: precision of the data
  //TO_INSERT: value of the data type
  //DB_READBACK: expected value read from database
  //HDFS_READBACK: expected value read from HDFS
  //NEG_POS_FLAG: mark if the test on the data type is expected to fail
  //OFFSET: line offset of the data in the input file
  enum KEY_STRINGS {
    SCALE, PREC, TO_INSERT, DB_READBACK, HDFS_READBACK, NEG_POS_FLAG, OFFSET,
  }

  private Map data;

  public String getData(KEY_STRINGS ks) {
    String ret;
    try {
      ret = data.get(ks).toString();
    } catch (Exception e) {
      return null;
    }
    return ret;
  }

  public void setData(KEY_STRINGS ks, String value) {
    this.data.put(ks, value);
  }

  public String toString() {
    String tmp = this.datatype;

    for (KEY_STRINGS a : KEY_STRINGS.values()) {

      tmp += "\n" + a.toString() + " : " + this.getData(a);
    }
    return tmp;
  }

  public int hashCode() {
    return super.hashCode();
  }

  public boolean equals(Object o) {
    int c1 = Integer.getInteger(this.getData(KEY_STRINGS.OFFSET));
    int c2 = Integer.getInteger(((MSSQLTestData) o)
        .getData(KEY_STRINGS.OFFSET));
    return (c1 == c2);
  }

  public int compareTo(Object o) {
    int c1 = Integer.getInteger(this.getData(KEY_STRINGS.OFFSET));
    int c2 = Integer.getInteger(((MSSQLTestData) o)
        .getData(KEY_STRINGS.OFFSET));
    return (c1 - c2);
  }

}
