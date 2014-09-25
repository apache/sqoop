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
package org.apache.sqoop.connector.idf.matcher;

import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;

public abstract class AbstractMatcher {

  //NOTE: This is currently tightly coupled to the CSV idf. We'll need refactoring after adding additional formats
  //NOTE: There's is a very blatant special case of empty schemas that seem to apply only to HDFS.

  /**
   *
   * @param fields
   * @param fromSchema
   * @param toSchema
   * @return Return the data in "fields" converted from matching the fromSchema to matching the toSchema.
   * Right not "converted" means re-ordering if needed and handling nulls.
   */
  abstract public String[] getMatchingData(String[] fields, Schema fromSchema, Schema toSchema);

  /***
   *
   * @param fromSchema
   * @param toSchema
   * @return return a schema with which to read the output data
   * This always returns the toSchema (since this is used when getting output data), unless its empty
   */
  public Schema getMatchingSchema(Schema fromSchema, Schema toSchema) {
    if (toSchema.isEmpty()) {
      return fromSchema;
    } else {
      return toSchema;
    }

  }

  protected boolean isNull(String value) {
    if (value.equals("NULL") || value.equals("null") || value.equals("'null'") || value.isEmpty()) {
      return true;
    }
    return false;
  }


}
