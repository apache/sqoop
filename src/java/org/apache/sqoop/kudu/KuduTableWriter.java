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

package org.apache.sqoop.kudu;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.util.*;

/**
 * Creates a Kudu table based on the schema of the source
 * table being imported.
 */
public class KuduTableWriter {

  public static final Log LOG = LogFactory.getLog(
      KuduTableWriter.class.getName());

  private KuduClient kuduClient;
  private SqoopOptions opts;
  private ConnManager connMgr;
  private String inputTable;
  private String outputTable;
  private String kuduKeyCols;
  private HashSet<String> keyColLookup;

  /**
   * Creates a new KuduTableWriter to create a Kudu table.
   *
   * @param opts        program-wide options
   * @param connMgr     the connection manager used to describe the table.
   * @param inputTable  the name of the table to load.
   * @param outputTable the name of the Kudu table to create.
   * @param config      the Hadoop configuration to use to connect to the dfs
   */
  public KuduTableWriter(final SqoopOptions opts,
                         final ConnManager connMgr,
                         final KuduClient kuduClient,
                         final String inputTable,
                         final String outputTable,
                         final Configuration config
  ) {
    this.opts = opts;
    this.connMgr = connMgr;
    this.kuduClient = kuduClient;
    this.inputTable = inputTable;
    this.outputTable = outputTable;
    this.kuduKeyCols = opts.getKuduKeyCols();
    this.keyColLookup = new HashSet<String>();
    extractKeyCols(); // memoize key cols
  }

  private Map<String, Integer> externalColTypes;

  /**
   * Set the column type map to be used.
   * (dependency injection for testing; not used in production.)
   */
  public void setColumnTypes(Map<String, Integer> colTypes) {
    this.externalColTypes = colTypes;
    LOG.debug("Using test-controlled type map");
  }

  /**
   * Get the column names to import.
   */
  private String[] getColumnNames() {
    String[] colNames = opts.getColumns();
    if (null != colNames) {
      return colNames; // user-specified column names.
    } else if (null != externalColTypes) {
      // Test-injection column mapping. Extract the col names from this.
      ArrayList<String> keyList = new ArrayList<String>();
      for (String key : externalColTypes.keySet()) {
        keyList.add(key);
      }

      return keyList.toArray(new String[keyList.size()]);
    } else if (null != inputTable) {
      return connMgr.getColumnNames(inputTable);
    } else {
      return connMgr.getColumnNamesForQuery(opts.getSqlQuery());
    }
  }

  /**
   * Retrieves Kudu Schema object for the new Kudu table.
   *
   * @return Schema for Kudu table.
   */
  private Schema getTableSchema() throws Exception {
    Map<String, Integer> columnTypes;
    Map<String, String> userMapping = new HashMap<String, String>();

    // Get user specified column mapping if any
    for (Map.Entry<Object, Object> prop : opts.getMapColumnKudu().entrySet()) {
      userMapping.put(prop.getKey().toString().trim().toUpperCase(),
          prop.getValue().toString().trim().toUpperCase());
    }

    if (externalColTypes != null) {
      // Use pre-defined column types.
      columnTypes = externalColTypes;
    } else {
      // Get these from the database.
      if (null != inputTable) {
        columnTypes = connMgr.getColumnTypes(inputTable);
      } else {
        columnTypes = connMgr.
            getColumnTypesForQuery(opts.getSqlQuery());
      }
    }
    if (keyColLookup.isEmpty()) {
      throw new Exception(
          "Kudu create table requires at least one key column"
      );
    }

    String[] colNames = getColumnNames();

    // Check that all explicitly mapped columns are present in result set
    for (String column : userMapping.keySet()) {
      boolean found = false;
      for (String c : colNames) {
        if (c.equalsIgnoreCase(column)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException(
            "No column by the name " + column
                + " found while importing data");
      }
    }

    // Check that the keyColumns are present in the result set
    for (String keyCol : keyColLookup) {
      boolean found = false;
      for (String c : colNames) {
        if (c.equalsIgnoreCase(keyCol)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("No key column by the name "
            + keyCol + "found while importing data");
      }
    }

    int numberOfCols = colNames.length;
    List<ColumnSchema> columns =
        new ArrayList<ColumnSchema>(numberOfCols);

    for (String col : colNames) {

      Integer colType = columnTypes.get(col);
      Type kuduColType = null;
      String mappedKuduColType
          = userMapping.get(col.toUpperCase());

      // Does a mapping exist in user specified map?
      if (mappedKuduColType != null) {
        LOG.debug("Attempting to map data type for column: "
            + col.toUpperCase());
        try {
          kuduColType = Type.valueOf(mappedKuduColType);
        } catch (IllegalArgumentException e) {
          LOG.warn("Column " + col.toUpperCase()
              + " cannot be mapped to type: " + mappedKuduColType);
          LOG.warn("Only the following types are supported: ");
          for (Type t : Type.values()) {
            LOG.warn("      " + t.getName().toUpperCase());
          }
        }
        if (kuduColType != null) {
          LOG.info("Mapped column " + col.toUpperCase()
              + " to type: " + kuduColType.getName().toUpperCase());
        }
      }

      if (kuduColType == null) {
        kuduColType = connMgr.toKuduType(inputTable, col, colType);
      }

      if (null == kuduColType) {
        throw new IOException(
            "Kudu does not support the SQL type for column " + col
        );
      }

      if (KuduTypes.isKuduTypeImprovised(colType)) {
        LOG.warn(
            "Column " + col.toUpperCase()
                + " had to be cast to a less precise type in Kudu"
        );
      }

      boolean isKeyColumn = keyColLookup.contains(col);

      // Key columns shouldnt be nullable
      boolean isNullable =
          (isKeyColumn) ? false
              : KuduConstants.KUDU_SET_NULLABLE_COLUMN_ALWAYS;
      if (isKeyColumn) {
        LOG.debug("Column " + col.toUpperCase()
            + " is marked as key column");
      }


      ColumnSchema columnSchema = new ColumnSchema
          .ColumnSchemaBuilder(col, kuduColType)
          .key(isKeyColumn)
          .nullable(isNullable)
          .build();
      columns.add(columnSchema);
    }

    return new Schema(columns);
  }

  /**
   * Creates a new Kudu Table based on the Schema
   * generated from the input source table/query.
   *
   * @throws IOException
   */
  public void createKuduTable() throws IOException {

    try {

      Schema schema = getTableSchema();
      if (null != schema) {
        printSchema(schema);
      }

      CreateTableOptions createTableOptions =
          new CreateTableOptions();

      // Set a replica count only if user explicity calls for it using
      // --kudu-replica-count
      // otherwise Kudu will pick the system default
      if (opts.getKuduReplicaCount() != null) {
        int replicaCount =
            Integer.parseInt(opts.getKuduReplicaCount());
        LOG.warn("Setting Kudu replica count to " + replicaCount);
        createTableOptions.setNumReplicas(replicaCount);
      }

      List<String> hashPartitionColumns =
          getPartitionKeyCols(opts.getKuduPartitionCols());
      int kuduPartitionBuckets =
          Integer.parseInt(opts.getKuduPartitionBuckets());

      createTableOptions.addHashPartitions(
          hashPartitionColumns,
          kuduPartitionBuckets
      );
      kuduClient.createTable(outputTable, schema, createTableOptions);

    } catch (Exception e) {
      LOG.error("Error creating Kudu table: " + this.outputTable);
      LOG.error(e.getMessage());
      throw new IOException("Error creating Kudu table: "
          + this.outputTable + " with exception: "
          + e.getMessage()
      );
    }
  }

  private void printSchema(Schema schema) {
    if (schema == null) {
      return;
    }

    LOG.debug("Printing schema for Kudu table..");
    for (ColumnSchema sch : schema.getColumns()) {
      LOG.debug("Column Name: " + sch.getName()
          + " [" + sch.getType().getName() + "]"
          + " key column: [" + sch.isKey() + "]"
      );
    }
  }

  /**
   * Loop through kuduKeyCols and memoize results.
   */
  private void extractKeyCols() {
    for (String keyCol : kuduKeyCols.split(
        KuduConstants.KUDU_KEY_COLS_DELIMITER)) {
      keyColLookup.add(keyCol);
    }
  }


  /**
   * Convert comma separated list of partition cols to a list of strings.
   */
  private List<String> getPartitionKeyCols(String columns) {
    String[] cols = columns.split(KuduConstants.KUDU_KEY_COLS_DELIMITER);
    List<String> partitionColList = new ArrayList<String>(cols.length);
    for (String col : cols) {
      LOG.info("Adding partition column: " + col);
      partitionColList.add(col);
    }
    return partitionColList;
  }

}
