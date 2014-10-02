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
package org.apache.sqoop.connector.matcher;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.SchemaError;
import org.apache.sqoop.schema.type.Column;

import java.util.HashMap;

public class NameMatcher extends Matcher {

  public static final Logger LOG = Logger.getLogger(NameMatcher.class);

  public NameMatcher(Schema from, Schema to) {
    super(from, to);
  }

  @Override
  public Object[] getMatchingData(Object[] fields) {
    Object[] out = new Object[getToSchema().getColumns().size()];

    HashMap<String,Column> colNames = new HashMap<String, Column>();

    for (Column fromCol: getFromSchema().getColumns()) {
      colNames.put(fromCol.getName(), fromCol);
    }

    int toIndex = 0;

    for (Column toCol: getToSchema().getColumns()) {
      Column fromCol = colNames.get(toCol.getName());

      if (fromCol != null) {
        int fromIndex = getFromSchema().getColumns().indexOf(fromCol);
        if (isNull(fields[fromIndex])) {
          out[toIndex] = null;
        } else {
          out[toIndex] = fields[fromIndex];
        }
      } else {
        //column exists in TO schema but not in FROM schema
        if (toCol.getNullable() == false) {
          throw new SqoopException(SchemaError.SCHEMA_0004,"target column " + toCol + " didn't match with any source column and cannot be null");
        } else {
          LOG.warn("Column " + toCol + " has no matching source column. Will be ignored. ");
          out[toIndex] = null;
        }
      }

      toIndex++;
    }

  return out;
  }

}
