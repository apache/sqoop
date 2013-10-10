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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.mapreduce.ImportJobBase;

import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.ProcessingException;

/**
 * SqoopRecordProcessor that performs an HBase "put" operation
 * that contains all the fields of the record.
 */
public class HBasePutProcessor implements Closeable, Configurable,
    FieldMapProcessor {

  public static final Log LOG = LogFactory.getLog(
      HBasePutProcessor.class.getName());

  /** Configuration key specifying the table to insert into. */
  public static final String TABLE_NAME_KEY = "sqoop.hbase.insert.table";

  /** Configuration key specifying the column family to insert into. */
  public static final String COL_FAMILY_KEY =
      "sqoop.hbase.insert.column.family";

  /** Configuration key specifying the column of the input whose value
   * should be used as the row id.
   */
  public static final String ROW_KEY_COLUMN_KEY =
      "sqoop.hbase.insert.row.key.column";

  /**
   * Configuration key specifying the PutTransformer implementation to use.
   */
  public static final String TRANSFORMER_CLASS_KEY =
      "sqoop.hbase.insert.put.transformer.class";

  /**
   *  Configuration key to enable/disable hbase bulkLoad.
   */
  public static final String BULK_LOAD_ENABLED_KEY =
      "sqoop.hbase.bulk.load.enabled";

  /** Configuration key to specify whether to add the row key column into
   *  HBase. Set to false by default.
   */
  public static final String ADD_ROW_KEY = "sqoop.hbase.add.row.key";
  public static final boolean ADD_ROW_KEY_DEFAULT = false;

  private Configuration conf;

  // An object that can transform a map of fieldName->object
  // into a Put command.
  private PutTransformer putTransformer;

  private String tableName;
  private HTable table;

  public HBasePutProcessor() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setConf(Configuration config) {
    this.conf = config;

    // Get the implementation of PutTransformer to use.
    // By default, we call toString() on every non-null field.
    Class<? extends PutTransformer> xformerClass =
        (Class<? extends PutTransformer>)
        this.conf.getClass(TRANSFORMER_CLASS_KEY, ToStringPutTransformer.class);
    this.putTransformer = (PutTransformer)
        ReflectionUtils.newInstance(xformerClass, this.conf);
    if (null == putTransformer) {
      throw new RuntimeException("Could not instantiate PutTransformer.");
    }

    this.putTransformer.setColumnFamily(conf.get(COL_FAMILY_KEY, null));
    this.putTransformer.setRowKeyColumn(conf.get(ROW_KEY_COLUMN_KEY, null));

    if (this.putTransformer instanceof ToStringPutTransformer) {
      ToStringPutTransformer stringPutTransformer =
          (ToStringPutTransformer) this.putTransformer;
      stringPutTransformer.bigDecimalFormatString =
          conf.getBoolean(ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
              ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
      stringPutTransformer.addRowKey =
          conf.getBoolean(HBasePutProcessor.ADD_ROW_KEY,
              HBasePutProcessor.ADD_ROW_KEY_DEFAULT);
      stringPutTransformer.detectCompositeKey();
    }

    this.tableName = conf.get(TABLE_NAME_KEY, null);
    try {
      this.table = new HTable(conf, this.tableName);
    } catch (IOException ioe) {
      throw new RuntimeException("Could not access HBase table " + tableName,
          ioe);
    }
    this.table.setAutoFlush(false);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  /**
   * Processes a record by extracting its field map and converting
   * it into a list of Put commands into HBase.
   */
  public void accept(FieldMappable record)
      throws IOException, ProcessingException {
    Map<String, Object> fields = record.getFieldMap();

    List<Put> putList = putTransformer.getPutCommand(fields);
    if (null != putList) {
      for (Put put : putList) {
        if (put!=null) {
          if (put.isEmpty()) {
            LOG.warn("Could not insert row with no columns "
                + "for row-key column: " + Bytes.toString(put.getRow()));
          } else {
            this.table.put(put);
          }
        }
      }
    }
  }

  @Override
  /**
   * Closes the HBase table and commits all pending operations.
   */
  public void close() throws IOException {
    this.table.flushCommits();
    this.table.close();
  }

}
