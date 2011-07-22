/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * PutTransformer that calls toString on all non-null fields.
 */
public class ToStringPutTransformer extends PutTransformer {

  public static final Log LOG = LogFactory.getLog(
      ToStringPutTransformer.class.getName());

  // A mapping from field name -> bytes for that field name.
  // Used to cache serialization work done for fields names.
  private Map<String, byte[]> serializedFieldNames;

  public ToStringPutTransformer() {
    serializedFieldNames = new TreeMap<String, byte[]>();
  }

  /**
   * Return the serialized bytes for a field name, using
   * the cache if it's already in there.
   */
  private byte [] getFieldNameBytes(String fieldName) {
    byte [] cachedName = serializedFieldNames.get(fieldName);
    if (null != cachedName) {
      // Cache hit. We're done.
      return cachedName;
    }

    // Do the serialization and memoize the result.
    byte [] nameBytes = Bytes.toBytes(fieldName);
    serializedFieldNames.put(fieldName, nameBytes);
    return nameBytes;
  }

  @Override
  /** {@inheritDoc} */
  public List<Put> getPutCommand(Map<String, Object> fields)
      throws IOException {

    String rowKeyCol = getRowKeyColumn();
    String colFamily = getColumnFamily();
    byte [] colFamilyBytes = Bytes.toBytes(colFamily);

    Object rowKey = fields.get(rowKeyCol);
    if (null == rowKey) {
      // If the row-key column is null, we don't insert this row.
      LOG.warn("Could not insert row with null value for row-key column: "
          + rowKeyCol);
      return null;
    }

    Put put = new Put(Bytes.toBytes(rowKey.toString()));

    for (Map.Entry<String, Object> fieldEntry : fields.entrySet()) {
      String colName = fieldEntry.getKey();
      if (!colName.equals(rowKeyCol)) {
        // This is a regular field, not the row key.
        // Add it if it's not null.
        Object val = fieldEntry.getValue();
        if (null != val) {
          put.add(colFamilyBytes, getFieldNameBytes(colName),
              Bytes.toBytes(val.toString()));
        }
      }
    }

    return Collections.singletonList(put);
  }
}
