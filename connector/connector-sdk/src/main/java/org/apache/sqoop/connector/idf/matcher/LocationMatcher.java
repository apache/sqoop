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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormatError;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.SchemaError;
import org.apache.sqoop.schema.SchemaMatchOption;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Type;

import java.math.BigDecimal;
import java.util.Iterator;


/**
 * Convert data according to FROM schema to data according to TO schema
 * This is done based on column location
 * So data in first column in FROM goes into first column in TO, etc
 * If TO schema has more fields and they are "nullable", the value will be set to null
 * If TO schema has extra non-null fields, we'll throw an exception
 */
public class LocationMatcher extends AbstractMatcher {

  public static final Logger LOG = Logger.getLogger(LocationMatcher.class);
  @Override
  public String[] getMatchingData(String[] fields, Schema fromSchema, Schema toSchema) {

    String[] out = new String[toSchema.getColumns().size()];

    int i = 0;

    if (toSchema.isEmpty()) {
      // If there's no destination schema, no need to convert anything
      // Just use the original data
      return fields;
    }

    for (Column col: toSchema.getColumns())
    {
      if (i < fields.length) {
        if (isNull(fields[i])) {
          out[i] = null;
        } else {
          out[i] = fields[i];
        }
      }
      // We ran out of fields before we ran out of schema
      else {
        if (!col.getNullable()) {
          throw new SqoopException(SchemaError.SCHEMA_0004,"target column " + col + " didn't match with any source column and cannot be null");
        } else {
          LOG.warn("Column " + col + " has no matching source column. Will be ignored. ");
          out[i] = null;
        }
      }
      i++;
    }
    return out;
  }


}
