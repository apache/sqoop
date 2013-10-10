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

package org.apache.sqoop.hbase;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.hbase.PutTransformer;

/**
 * PutTransformer that calls toString on all non-null fields.
 */
public class ToStringPutTransformer extends PutTransformer {

  public static final Log LOG = LogFactory.getLog(
      ToStringPutTransformer.class.getName());

  // A mapping from field name -> bytes for that field name.
  // Used to cache serialization work done for fields names.
  private Map<String, byte[]> serializedFieldNames;
  protected boolean bigDecimalFormatString;
  protected boolean addRowKey;
  private boolean isCompositeKey = false;
  private List<String> compositeKeyAttributes;

  /**
   * Used as delimiter to combine composite-key column names when passed as.
   * argument to --hbase-row-key
   */
  public static final String DELIMITER_COMMAND_LINE = ",";

  /**
   * Used as connecting char for storing composite-key values to form.
   * composite row-key on hbase
   */
  public static final String DELIMITER_HBASE = "_";

  public ToStringPutTransformer() {
    serializedFieldNames = new TreeMap<String, byte[]>();
  }

  /**
   * Return the serialized bytes for a field name, using.
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

  /**
   * Checks whether --hbase-row-key parameter is a comma separated list of.
   * attributes i.e composite key
   */
  public void detectCompositeKey() {
    String rowKeyCol = getRowKeyColumn();
    if (null != rowKeyCol && rowKeyCol.contains(DELIMITER_COMMAND_LINE)) {
      // Set the flag as true
      isCompositeKey = true;
      String[] compositeKeyArray = rowKeyCol.split(DELIMITER_COMMAND_LINE);
      compositeKeyAttributes = Arrays.asList(compositeKeyArray);
    }
  }

  @Override
  /** {@inheritDoc} */
  public List<Put> getPutCommand(Map<String, Object> fields)
      throws IOException {

    String rowKeyCol = getRowKeyColumn();
    if (null == rowKeyCol) {
      throw new IOException("Row key column can't be NULL.");
    }

    String colFamily = getColumnFamily();
    if (null == colFamily) {
      throw new IOException("Column family can't be NULL.");
    }

    if (isCompositeKey) {
      // Indicates row-key is a composite key (multiple attribute key)
      List<String> rowKeyList = new ArrayList<String>();

      // storing each comma-separated attribute into list
      for (String fieldName : compositeKeyAttributes) {
        Object fieldValue = fields.get(fieldName);
        if (null == fieldValue) {
          // If the row-key column value is null, we don't insert this row.
          throw new IOException("Could not insert row with null "
            + "value for row-key column: " + fieldName);
        }
        String rowKey = toHBaseString(fieldValue);
        // inserting value of each attribute (rowKey) into list
        rowKeyList.add(rowKey);
      }

      // construct rowKey by combining attribute values
      // from composite key
      String compositeRowKey = StringUtils.join(DELIMITER_HBASE, rowKeyList);
      // Insert record in HBase
      return putRecordInHBase(fields, colFamily, compositeRowKey);

    } else {
      // if row-key is regular primary key
      // i.e. it contains only one attribute

      Object rowKey = fields.get(rowKeyCol);
      if (null == rowKey) {
        // If the row-key column is null, we don't insert this row.
        throw new IOException("Could not insert row with null "
          + "value for row-key column: " + rowKeyCol);
      }

      String hBaseRowKey = toHBaseString(rowKey);
      return putRecordInHBase(fields, colFamily, hBaseRowKey);
   }
 }

  /**
   * Performs actual Put operation for the specified record in HBase.
   * @param record
   * @param colFamily
   * @param rowKey
   * @return List containing a single put command
   */
  private List<Put> putRecordInHBase(Map<String, Object> record,
    String colFamily, String rowKey) {
    // Put row-key in HBase
    Put put = new Put(Bytes.toBytes(rowKey));
    byte[] colFamilyBytes = Bytes.toBytes(colFamily);

    for (Map.Entry<String, Object> fieldEntry : record.entrySet()) {
      String colName = fieldEntry.getKey();
      boolean rowKeyCol = false;
      /*
       * For both composite key and normal primary key,
       * check if colName is part of rowKey.
       */
      if ((isCompositeKey && compositeKeyAttributes.contains(colName))
        || colName.equals(getRowKeyColumn())) {
        rowKeyCol = true;
      }

      if (!rowKeyCol || addRowKey) {
        // check addRowKey flag before including rowKey field.
        Object val = fieldEntry.getValue();
        if (null != val) {
          if ( val instanceof byte[]) {
            put.add(colFamilyBytes, getFieldNameBytes(colName),
                (byte[])val);
          } else {
	          put.add(colFamilyBytes, getFieldNameBytes(colName),
	              Bytes.toBytes(toHBaseString(val)));
          }
        }
      }
    }
    return Collections.singletonList(put);
  }

  private String toHBaseString(Object val) {
    String valString;
    if (val instanceof BigDecimal && bigDecimalFormatString) {
      valString = ((BigDecimal) val).toPlainString();
    } else {
      valString = val.toString();
    }
    return valString;
  }

}
