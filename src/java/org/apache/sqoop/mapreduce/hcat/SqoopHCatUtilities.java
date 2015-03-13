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

package org.apache.sqoop.mapreduce.hcat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HCatHadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.hive.HiveTypes;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.util.Executor;
import org.apache.sqoop.util.LoggingAsyncSink;
import org.apache.sqoop.util.SubprocessSecurityManager;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.util.ExitSecurityException;

/**
 * Utility methods for the HCatalog support for Sqoop.
 */
public final class SqoopHCatUtilities {
  public static final String DEFHCATDB = "default";
  public static final String HIVESITEXMLPATH = "/conf/hive-site.xml";
  public static final String HCATSHAREDIR = "share/hcatalog";
  public static final String DEFLIBDIR = "lib";
  public static final String TEXT_FORMAT_IF_CLASS =
    "org.apache.hadoop.mapred.TextInputFormat";
  public static final String TEXT_FORMAT_OF_CLASS =
    "org.apache.hadoop.mapred.TextOutputFormat";
  public static final String TEXT_FORMAT_SERDE_CLASS =
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  public static final String HCAT_DB_OUTPUT_COLTYPES_JAVA =
    "sqoop.hcat.db.output.coltypes.java";
  public static final String HCAT_DB_OUTPUT_COLTYPES_SQL =
    "sqoop.hcat.db.output.coltypes.sql";
  public static final String HCAT_CLI_MAIN_CLASS =
    "org.apache.hive.hcatalog.cli.HCatCli";
  public static final String HCAT_DEF_STORAGE_STANZA = "stored as rcfile";
  public static final String HIVE_DELIMITERS_TO_REPLACE_PROP =
    "sqoop.hive.delims.to.replace";
  public static final String HIVE_DELIMITERS_REPLACEMENT_PROP =
    "sqoop.hive.delims.replacement";
  public static final String HIVE_DELIMITERS_REPLACEMENT_ENABLED_PROP =
    "sqoop.hive.delims.replacement.enabled";
  public static final String HCAT_STATIC_PARTITION_KEY_PROP =
    "sqoop.hcat.partition.key";
  public static final String HCAT_FIELD_POSITIONS_PROP =
    "sqoop.hcat.field.positions";
  public static final String DEBUG_HCAT_IMPORT_MAPPER_PROP =
    "sqoop.hcat.debug.import.mapper";
  public static final String DEBUG_HCAT_EXPORT_MAPPER_PROP =
    "sqoop.hcat.debug.export.mapper";
  private static final String HCATCMD = Shell.WINDOWS ? "hcat.py" : "hcat";
  private static final int MAX_HIVE_CHAR_PREC = 65535;
  private static final int MAX_HIVE_DECIMAL_PREC = 65;
  private static final int MAX_HIVE_DECIMAL_SCALE = 30;
  private SqoopOptions options;
  private ConnManager connManager;
  private String hCatTableName;
  private String hCatDatabaseName;
  private Configuration configuration;
  private Job hCatJob;
  private HCatSchema hCatOutputSchema;
  private HCatSchema hCatPartitionSchema;
  private HCatSchema projectedSchema;
  private boolean configured;

  private String hCatQualifiedTableName;
  private List<String> hCatStaticPartitionKeys;
  private List<String> hCatStaticPartitionValues;
  private List<String> hCatDynamicPartitionKeys;
  // DB stuff
  private String[] dbColumnNames;
  private String dbTableName;
  private LCKeyMap<List<Integer>> dbColumnInfo;

  private int[] hCatFieldPositions; // For each DB column, HCat position

  private HCatSchema hCatFullTableSchema;
  private List<String> hCatFullTableSchemaFieldNames;
  private LCKeyMap<String> userHiveMapping;

  // For testing support
  private static Class<? extends InputFormat> inputFormatClass =
    SqoopHCatExportFormat.class;

  private static Class<? extends OutputFormat> outputFormatClass =
    HCatOutputFormat.class;

  private static Class<? extends Mapper> exportMapperClass =
    SqoopHCatExportMapper.class;

  private static Class<? extends Mapper> importMapperClass =
    SqoopHCatImportMapper.class;

  private static Class<? extends Writable> importValueClass =
    DefaultHCatRecord.class;

  private static boolean testMode = false;

  static class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
      super(IntWritable.class);
    }
  }

  /**
   * A Map using String as key type that ignores case of its key and stores the
   * key in lower case.
   */
  private static class LCKeyMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -6751510232323094216L;

    @Override
    public V put(String key, V value) {
      return super.put(key.toLowerCase(), value);
    }

    @Override
    public V get(Object key) {
      return super.get(((String) key).toLowerCase());
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> map) {
      for(Map.Entry<? extends String, ? extends V> entry: map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * A Map using String as key type that ignores case of its key and stores the
   * key in upper case.
   */
  public class UCKeyMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -6751510232323094216L;

    @Override
    public V put(String key, V value) {
      return super.put(key.toUpperCase(), value);
    }

    @Override
    public V get(Object key) {
      return super.get(((String) key).toUpperCase());
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> map) {
      for(Map.Entry<? extends String, ? extends V> entry: map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * A class to hold the instance. For guaranteeing singleton creation using JMM
   * semantics.
   */
  public static final class Holder {
    @SuppressWarnings("synthetic-access")
    public static final SqoopHCatUtilities INSTANCE = new SqoopHCatUtilities();

    private Holder() {
    }
  }

  public static SqoopHCatUtilities instance() {
    return Holder.INSTANCE;
  }

  private SqoopHCatUtilities() {
    configured = false;
  }

  public static final Log LOG = LogFactory.getLog(SqoopHCatUtilities.class
    .getName());

  public boolean isConfigured() {
    return configured;
  }

  private void checkHomeDirs(SqoopOptions opts) {

    LOG.info("Configuring HCatalog specific details for job");

    String home = opts.getHiveHome();

    if (home == null || home.length() == 0) {
      LOG.warn("Hive home is not set. job may fail if needed jar files "
        + "are not found correctly.  Please set HIVE_HOME in"
        + " sqoop-env.sh or provide --hive-home option.  HIVE_HOME defaulted "
        + " to " + SqoopOptions.getHiveHomeDefault());
    }

    home = opts.getHCatHome();
    if (home == null || home.length() == 0) {
      LOG.warn("HCatalog home is not set. job may fail if needed jar "
        + "files are not found correctly.  Please set HCAT_HOME in"
        + " sqoop-env.sh or provide --hcatalog-home option.  "
        + "HCAT_HOME defaulted to " + SqoopOptions.getHCatHomeDefault());
    }
  }

  public void configureHCat(final SqoopOptions opts, final Job job,
    final ConnManager connMgr, final String dbTable,
    final Configuration config) throws IOException {
    if (configured) {
      LOG.info("Ignoring configuration request for HCatalog info");
      return;
    }
    options = opts;
    checkHomeDirs(opts);
    connManager = connMgr;
    dbTableName = dbTable;
    configuration = config;
    hCatJob = job;
    hCatDatabaseName = options.getHCatDatabaseName() != null ? options
      .getHCatDatabaseName() : DEFHCATDB;
    hCatDatabaseName = hCatDatabaseName.toLowerCase();

    String optHCTabName = options.getHCatTableName();
    hCatTableName = optHCTabName.toLowerCase();

    if (!hCatTableName.equals(optHCTabName)) {
      LOG.warn("Provided HCatalog table name " + optHCTabName
        + " will be mapped to  " + hCatTableName);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(hCatDatabaseName);
    sb.append('.').append(hCatTableName);
    hCatQualifiedTableName = sb.toString();

    String principalID = System
      .getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null) {
      configuration.set(HCatConstants.HCAT_METASTORE_PRINCIPAL, principalID);
    }
    hCatStaticPartitionKeys = new ArrayList<String>();
    hCatStaticPartitionValues = new ArrayList<String>();
    String partKeysString = options.getHCatalogPartitionKeys();
    String partKeysVals = options.getHCatalogPartitionValues();
    // Already validated
    if (partKeysString != null) {
      String[] keys = partKeysString.split(",");
      for (int i = 0; i < keys.length; ++i) {
        String k = keys[i].trim();
        hCatStaticPartitionKeys.add(k);
      }
      String[] vals = partKeysVals.split(",");
      for (int i = 0; i < vals.length; ++i) {
        String v = vals[i].trim();
        hCatStaticPartitionValues.add(v);
      }
    } else {
      partKeysString = options.getHivePartitionKey();
      if (partKeysString != null) {
        hCatStaticPartitionKeys.add(partKeysString);
      }
      partKeysVals = options.getHivePartitionValue();
      hCatStaticPartitionValues.add(partKeysVals);

    }
    Properties userMapping = options.getMapColumnHive();
    userHiveMapping = new LCKeyMap<String>();
    for (Object o : userMapping.keySet()) {
      String v = (String) userMapping.get(o);
      userHiveMapping.put((String) o, v);
    }
    // Get the partition key filter if needed
    Map<String, String> filterMap = getHCatSPFilterMap();
    String filterStr = getHCatSPFilterStr();
    initDBColumnInfo();
    if (options.doCreateHCatalogTable()) {
      LOG.info("Creating HCatalog table " + hCatQualifiedTableName
        + " for import");
      createHCatTable();
    }
    // For serializing the schema to conf
    HCatInputFormat hif = HCatInputFormat.setInput(hCatJob, hCatDatabaseName,
      hCatTableName);
    // For serializing the schema to conf
    if (filterStr != null) {
      LOG.info("Setting hCatInputFormat filter to " + filterStr);
      hif.setFilter(filterStr);
    }

    hCatFullTableSchema = HCatInputFormat.getTableSchema(configuration);
    hCatFullTableSchemaFieldNames = hCatFullTableSchema.getFieldNames();

    LOG.info("HCatalog full table schema fields = "
      + Arrays.toString(hCatFullTableSchema.getFieldNames().toArray()));

    if (filterMap != null) {
      LOG.info("Setting hCatOutputFormat filter to " + filterStr);
    }

    HCatOutputFormat.setOutput(hCatJob,
      OutputJobInfo.create(hCatDatabaseName, hCatTableName, filterMap));
    hCatOutputSchema = HCatOutputFormat.getTableSchema(configuration);
    List<HCatFieldSchema> hCatPartitionSchemaFields =
      new ArrayList<HCatFieldSchema>();
    int totalFieldsCount = hCatFullTableSchema.size();
    int dataFieldsCount = hCatOutputSchema.size();
    if (totalFieldsCount > dataFieldsCount) {
      for (int i = dataFieldsCount; i < totalFieldsCount; ++i) {
        hCatPartitionSchemaFields.add(hCatFullTableSchema.get(i));
      }
    }

    hCatPartitionSchema = new HCatSchema(hCatPartitionSchemaFields);
    for (HCatFieldSchema hfs : hCatPartitionSchemaFields) {
      if (hfs.getType() != HCatFieldSchema.Type.STRING) {
        throw new IOException("The table provided "
          + getQualifiedHCatTableName()
          + " uses unsupported  partitioning key type  for column "
          + hfs.getName() + " : " + hfs.getTypeString() + ".  Only string "
          + "fields are allowed in partition columns in HCatalog");
      }

    }
    LOG.info("HCatalog table partitioning key fields = "
      + Arrays.toString(hCatPartitionSchema.getFieldNames().toArray()));

    List<HCatFieldSchema> outputFieldList = new ArrayList<HCatFieldSchema>();
    for (String col : dbColumnNames) {
      HCatFieldSchema hfs = hCatFullTableSchema.get(col);
      if (hfs == null) {
        throw new IOException("Database column " + col + " not found in "
          + " hcatalog table.");
      }
      boolean skip=false;
      if (hCatStaticPartitionKeys != null) {
        for (String key : hCatStaticPartitionKeys) {
          if (col.equals(key)) {
            skip=true;
            break;
          }
        }
      }
      if (skip) {
        continue;
      }
      outputFieldList.add(hCatFullTableSchema.get(col));
    }

    projectedSchema = new HCatSchema(outputFieldList);

    LOG.info("HCatalog projected schema fields = "
      + Arrays.toString(projectedSchema.getFieldNames().toArray()));

    validateStaticPartitionKey();
    validateHCatTableFieldTypes();

    HCatOutputFormat.setSchema(configuration, hCatFullTableSchema);

    addJars(hCatJob, options);
    config.setBoolean(DEBUG_HCAT_IMPORT_MAPPER_PROP,
      Boolean.getBoolean(DEBUG_HCAT_IMPORT_MAPPER_PROP));
    config.setBoolean(DEBUG_HCAT_EXPORT_MAPPER_PROP,
      Boolean.getBoolean(DEBUG_HCAT_EXPORT_MAPPER_PROP));
    configured = true;
  }

  public void validateDynamicPartitionKeysMapping() throws IOException {
    // Now validate all partition columns are in the database column list
    StringBuilder missingKeys = new StringBuilder();

    for (String s : hCatDynamicPartitionKeys) {
      boolean found = false;
      for (String c : dbColumnNames) {
        if (s.equals(c)) {
          found = true;
          break;
        }
      }
      if (!found) {
        missingKeys.append(',').append(s);
      }
    }
    if (missingKeys.length() > 0) {
      throw new IOException("Dynamic partition keys are not "
        + "present in the database columns.   Missing keys = "
        + missingKeys.substring(1));
    }
  }

  public void validateHCatTableFieldTypes() throws IOException {
    StringBuilder sb = new StringBuilder();
    boolean hasComplexFields = false;
    for (HCatFieldSchema hfs : projectedSchema.getFields()) {
      if (hfs.isComplex()) {
        sb.append('.').append(hfs.getName());
        hasComplexFields = true;
      }
    }

    if (hasComplexFields) {
      String unsupportedFields = sb.substring(1);
      throw new IOException("The HCatalog table provided "
        + getQualifiedHCatTableName() + " has complex field types ("
        + unsupportedFields + ").  They are currently not supported");
    }

  }

  /**
   * Get the column names to import.
   */
  private void initDBColumnInfo() throws IOException {
    String[] colNames = options.getColumns();
    if (null == colNames) {
      if (null != dbTableName) {
        colNames = connManager.getColumnNames(dbTableName);
      } else if (options.getCall() != null) {
        // Read procedure arguments from metadata
        colNames = connManager.getColumnNamesForProcedure(this.options
          .getCall());
      } else {
        colNames = connManager.getColumnNamesForQuery(options.getSqlQuery());
      }
    }

    dbColumnNames = new String[colNames.length];

    for (int i = 0; i < colNames.length; ++i) {
      dbColumnNames[i] = colNames[i].toLowerCase();
    }

    LCKeyMap<List<Integer>> colInfo = new LCKeyMap<List<Integer>>();
    if (dbTableName != null) {
        colInfo.putAll(connManager.getColumnInfo(dbTableName));
    } else if (options.getCall() != null) {
        // Read procedure arguments from metadata
        colInfo.putAll(connManager.getColumnInfoForProcedure(this.options
          .getCall()));
    } else {
        colInfo.putAll(connManager.getColumnInfoForQuery(options
          .getSqlQuery()));
    }

    if (options.getColumns() == null) {
      dbColumnInfo = colInfo;
    } else {
      dbColumnInfo = new LCKeyMap<List<Integer>>();
      // prune column types based on projection
      for (String col : dbColumnNames) {
        List<Integer> info = colInfo.get(col);
        if (info == null) {
          throw new IOException("Projected column " + col
            + " not in list of columns from database");
        }
        dbColumnInfo.put(col, info);
      }
    }
    LOG.info("Database column names projected : "
      + Arrays.toString(dbColumnNames));
    StringBuilder sb = new StringBuilder(1024);
    for (String col : dbColumnInfo.keySet()) {
      sb.append('\t').append(col).append(" : [");
      List<Integer> info = dbColumnInfo.get(col);
      sb.append("Type : ").append(info.get(0)).append(',');
      sb.append("Precision : ").append(info.get(1)).append(',');
      sb.append("Scale : ").append(info.get(2)).append(']');
      sb.append('\n');
    }
    LOG.info("Database column name - info map :\n" + sb.toString());
  }

  public static StringBuilder escHCatObj(String objectName) {
    StringBuilder sb = new StringBuilder(64);
    sb.append('`').append(objectName).append('`');
    return sb;
  }

  private void createHCatTable() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append("create table ").
      append(escHCatObj(hCatDatabaseName)).append('.');
    sb.append(escHCatObj(hCatTableName)).append(" (\n\t");
    boolean first = true;
    for (String col : dbColumnNames) {
      String type = userHiveMapping.get(col);
      int prec = -1;
      int scale = -1;
      if (type == null) {
        type = connManager.toHCatType(dbColumnInfo.get(col).get(0));
      }
      if (type.equals("char") || type.equals("varchar")) {
        prec = dbColumnInfo.get(col).get(1);
        if (prec > MAX_HIVE_CHAR_PREC) {
          LOG.warn("Truncating precison of column " + col + "  from " + prec
            + " to " + MAX_HIVE_CHAR_PREC);
          prec = MAX_HIVE_CHAR_PREC;
        }
      } else if (type.equals("decimal")) {
          prec = dbColumnInfo.get(col).get(1);
          if (prec > MAX_HIVE_DECIMAL_PREC) {
            LOG.warn("Truncating precison of column " + col + "  from " + prec
              + " to " + MAX_HIVE_DECIMAL_PREC);
            prec = MAX_HIVE_DECIMAL_PREC;
          }

          scale = dbColumnInfo.get(col).get(2);
          if (scale > MAX_HIVE_DECIMAL_SCALE) {
            LOG.warn("Truncating precison of column " + col + "  from " + scale
              + " to " + MAX_HIVE_DECIMAL_SCALE);
            scale = MAX_HIVE_DECIMAL_SCALE;
          }
      }
      boolean skip=false;
      if (hCatStaticPartitionKeys != null) {
        for (String key : hCatStaticPartitionKeys) {
          if (col.equals(key)) {
            skip=true;
            break;
          }
        }
      }
      if (skip) {
        continue;
      }
      if (first) {
        first = false;
      } else {
        sb.append(",\n\t");
      }
      sb.append(escHCatObj(col)).append(' ').append(type);
      if (prec > 0) {
        sb.append('(').append(prec);
        if (scale > 0) {
          sb.append(',').append(scale);
        }
        sb.append(')');
      }
    }
    sb.append(")\n");
    first = true;
    if (hCatStaticPartitionKeys != null && hCatStaticPartitionKeys.size() > 0) {
      sb.append("partitioned by (\n\t");
      for (String key : hCatStaticPartitionKeys) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(escHCatObj(key)).append(" string");
      }
      sb.append(")\n");
    }
    String storageStanza = options.getHCatStorageStanza();
    if (storageStanza == null) {
      sb.append(HCAT_DEF_STORAGE_STANZA);
    } else {
      sb.append(storageStanza);
    }
    String createStatement = sb.toString();
    LOG.info("HCatalog Create table statement: \n\n" + createStatement);
    // Always launch as an external program so that logging is not messed
    // up by the use of inline hive CLI except in tests
    // We prefer external HCAT client.
    launchHCatCli(createStatement);
  }


  private void validateFieldAndColumnMappings() throws IOException {
    // Check that all explicitly mapped columns are present
    for (Object column : userHiveMapping.keySet()) {
      boolean found = false;
      for (String c : dbColumnNames) {
        if (c.equalsIgnoreCase((String) column)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Column " + column
          + " not found while mapping database columns to hcatalog columns");
      }
    }

    hCatFieldPositions = new int[dbColumnNames.length];

    Arrays.fill(hCatFieldPositions, -1);

    for (int indx = 0; indx < dbColumnNames.length; ++indx) {
      boolean userMapped = false;
      String col = dbColumnNames[indx];
      List<Integer> colInfo = dbColumnInfo.get(col);
      String hCatColType = userHiveMapping.get(col);
      if (hCatColType == null) {
        LOG.debug("No user defined type mapping for HCatalog field " + col);
        hCatColType = connManager.toHCatType(colInfo.get(0));
      } else {
        LOG.debug("Found type mapping for HCatalog filed " + col);
        userMapped = true;
      }
      if (null == hCatColType) {
        throw new IOException("HCat does not support the SQL type for column "
          + col);
      }

      boolean found = false;
      for (String tf : hCatFullTableSchemaFieldNames) {
        if (tf.equals(col)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IOException("Database column " + col + " not found in "
          + "hcatalog table schema or partition schema");
      }
      if (!userMapped) {
        HCatFieldSchema hCatFS = hCatFullTableSchema.get(col);
        if (!hCatFS.getTypeString().equals(hCatColType)) {
          LOG.warn("The HCatalog field " + col + " has type "
            + hCatFS.getTypeString() + ".  Expected = " + hCatColType
            + " based on database column type : "
            + sqlTypeString(colInfo.get(0)));
          LOG.warn("The Sqoop job can fail if types are not "
            + " assignment compatible");
        }
      }

      if (HiveTypes.isHiveTypeImprovised(colInfo.get(0))) {
        LOG.warn("Column " + col + " had to be cast to a less precise type "
          + hCatColType + " in hcatalog");
      }
      hCatFieldPositions[indx] = hCatFullTableSchemaFieldNames.indexOf(col);
      if (hCatFieldPositions[indx] < 0) {
        throw new IOException("The HCatalog field " + col
          + " could not be found");
      }
    }

    IntWritable[] positions = new IntWritable[hCatFieldPositions.length];
    for (int i = 0; i < hCatFieldPositions.length; ++i) {
      positions[i] = new IntWritable(hCatFieldPositions[i]);
    }

    DefaultStringifier.storeArray(configuration, positions,
      HCAT_FIELD_POSITIONS_PROP);
  }

  private String getHCatSPFilterStr() {
    if (hCatStaticPartitionKeys != null && hCatStaticPartitionKeys.size() > 0) {
      StringBuilder filter = new StringBuilder();
      boolean first = true;
      for (int i = 0; i < hCatStaticPartitionKeys.size(); ++i) {
        String key = hCatStaticPartitionKeys.get(i);
        String val = hCatStaticPartitionValues.get(i);
        if (first) {
          first = false;
        } else {
          filter.append(" AND ");
        }
        filter.append(key).append('=').append('\'')
          .append(val).append('\'');
      }
      return filter.toString();
    }
    return null;
  }

  private Map<String, String> getHCatSPFilterMap() {
    if (hCatStaticPartitionKeys != null && hCatStaticPartitionKeys.size() > 0) {
      Map<String, String> filter = new HashMap<String, String>();
      for (int i = 0; i < hCatStaticPartitionKeys.size(); ++i) {
        String key = hCatStaticPartitionKeys.get(i);
        String val = hCatStaticPartitionValues.get(i);
        filter.put(key, val);
      }
      return filter;
    }
    return null;
  }

  private void validateStaticPartitionKey() throws IOException {
    // check the static partition key from command line
    List<HCatFieldSchema> partFields = hCatPartitionSchema.getFields();

    if (hCatStaticPartitionKeys != null
        && hCatStaticPartitionKeys.size() > 0) {
      for (String key : hCatStaticPartitionKeys) {
        boolean found = false;
        for (HCatFieldSchema hfs : partFields) {
          if (hfs.getName().equals(key)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new IOException("The provided hive partition key "
          + key + " is not part of the partition "
          + " keys for table " + getQualifiedHCatTableName());
        }
      }
    }
    hCatDynamicPartitionKeys = new ArrayList<String>();
    hCatDynamicPartitionKeys.addAll(hCatPartitionSchema.getFieldNames());
    if (hCatStaticPartitionKeys != null
        && hCatStaticPartitionKeys.size() > 0) {
      for (String key : hCatStaticPartitionKeys) {
        hCatDynamicPartitionKeys.remove(key);
      }
    }
    configuration.setStrings(HCAT_STATIC_PARTITION_KEY_PROP,
       hCatStaticPartitionKeys.toArray(
         new String[hCatStaticPartitionKeys.size()]));
  }

  public static void configureImportOutputFormat(SqoopOptions opts, Job job,
    ConnManager connMgr, String dbTable, Configuration config)
    throws IOException {

    LOG.info("Configuring HCatalog for import job");
    SqoopHCatUtilities.instance().configureHCat(opts, job, connMgr, dbTable,
      job.getConfiguration());
    LOG.info("Validating dynamic partition keys");
    SqoopHCatUtilities.instance().validateFieldAndColumnMappings();
    SqoopHCatUtilities.instance().validateDynamicPartitionKeysMapping();
    job.setOutputFormatClass(getOutputFormatClass());
    IntWritable[] delimChars = new IntWritable[5];
    String hiveReplacement = "";
    LOG.debug("Hive delimiters will be fixed during import");
    DelimiterSet delims = opts.getOutputDelimiters();
    if (!opts.explicitOutputDelims()) {
      delims = DelimiterSet.HIVE_DELIMITERS;
    }
    delimChars = new IntWritable[] {
      new IntWritable(delims.getFieldsTerminatedBy()),
      new IntWritable(delims.getLinesTerminatedBy()),
      new IntWritable(delims.getEnclosedBy()),
      new IntWritable(delims.getEscapedBy()),
      new IntWritable(delims.isEncloseRequired() ? 1 : 0), };
    hiveReplacement = opts.getHiveDelimsReplacement();
    if (hiveReplacement == null) {
      hiveReplacement = "";
    }

    LOG.debug("Setting hive delimiters information");
    DefaultStringifier.storeArray(config, delimChars,
      HIVE_DELIMITERS_TO_REPLACE_PROP);
    config.set(HIVE_DELIMITERS_REPLACEMENT_PROP, hiveReplacement);
    if (opts.doHiveDropDelims() || opts.getHiveDelimsReplacement() != null) {
      LOG.debug("Enabling hive delimter replacement");
      config.set(HIVE_DELIMITERS_REPLACEMENT_ENABLED_PROP, "true");
    } else {
      LOG.debug("Disabling hive delimter replacement");
      config.set(HIVE_DELIMITERS_REPLACEMENT_ENABLED_PROP, "false");
    }
  }

  public static void configureExportInputFormat(SqoopOptions opts, Job job,
    ConnManager connMgr, String dbTable, Configuration config)
    throws IOException {

    LOG.info("Configuring HCatalog for export job");
    SqoopHCatUtilities hCatUtils = SqoopHCatUtilities.instance();
    hCatUtils
      .configureHCat(opts, job, connMgr, dbTable, job.getConfiguration());
    job.setInputFormatClass(getInputFormatClass());
    Map<String, List<Integer>> dbColInfo = hCatUtils.getDbColumnInfo();
    MapWritable columnTypesJava = new MapWritable();
    Properties mapColumnJava = opts.getMapColumnJava();
    for (Map.Entry<String, List<Integer>> e : dbColInfo.entrySet()) {
      Text columnName = new Text(e.getKey());
      Text columnText = null;
      if (mapColumnJava.containsKey(e.getKey())) {
        columnText = new Text(mapColumnJava.getProperty(e.getKey()));
      } else {
        columnText = new Text(connMgr.toJavaType(dbTable, e.getKey(),
          e.getValue().get(0)));
      }
      columnTypesJava.put(columnName, columnText);
    }
    MapWritable columnTypesSql = new MapWritable();
    for (Map.Entry<String, List<Integer>> e : dbColInfo.entrySet()) {
      Text columnName = new Text(e.getKey());
      IntWritable sqlType = new IntWritable(e.getValue().get(0));
      columnTypesSql.put(columnName, sqlType);
    }
    DefaultStringifier.store(config, columnTypesJava,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_JAVA);
    DefaultStringifier.store(config, columnTypesSql,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_SQL);
  }

  /**
   * Add the Hive and HCatalog jar files to local classpath and dist cache.
   * @throws IOException
   */
  public static void addJars(Job job, SqoopOptions options) throws IOException {

    if (ConfigurationHelper.isLocalJobTracker(job.getConfiguration())) {
      LOG.info("Not adding hcatalog jars to distributed cache in local mode");
      return;
    }
    if (options.isSkipDistCache()) {
      LOG.info("Not adding hcatalog jars to distributed cache as requested");
      return;
    }
    Configuration conf = job.getConfiguration();
    String hiveHome = null;
    String hCatHome = null;
    FileSystem fs = FileSystem.getLocal(conf);
    if (options != null) {
      hiveHome = options.getHiveHome();
    }
    if (hiveHome == null) {
      hiveHome = SqoopOptions.getHiveHomeDefault();
    }
    if (options != null) {
      hCatHome = options.getHCatHome();
    }
    if (hCatHome == null) {
      hCatHome = SqoopOptions.getHCatHomeDefault();
    }
    LOG.info("HCatalog job : Hive Home = " + hiveHome);
    LOG.info("HCatalog job:  HCatalog Home = " + hCatHome);

    conf.addResource(hiveHome + HIVESITEXMLPATH);

    // Add these to the 'tmpjars' array, which the MR JobSubmitter
    // will upload to HDFS and put in the DistributedCache libjars.
    List<String> libDirs = new ArrayList<String>();
    libDirs.add(hCatHome + File.separator + HCATSHAREDIR);
    libDirs.add(hCatHome + File.separator + DEFLIBDIR);
    libDirs.add(hiveHome + File.separator + DEFLIBDIR);
    Set<String> localUrls = new HashSet<String>();
    // Add any libjars already specified
    localUrls
      .addAll(conf
        .getStringCollection(
        ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM));
    for (String dir : libDirs) {
      LOG.info("Adding jar files under " + dir + " to distributed cache");
      addDirToCache(new File(dir), fs, localUrls, false);
    }

    // Recursively add all hcatalog storage handler jars
    // The HBase storage handler is getting deprecated post Hive+HCat merge
    String hCatStorageHandlerDir = hCatHome + File.separator
      + "share/hcatalog/storage-handlers";
    LOG.info("Adding jar files under " + hCatStorageHandlerDir
      + " to distributed cache (recursively)");

    addDirToCache(new File(hCatStorageHandlerDir), fs, localUrls, true);

    String tmpjars = conf
      .get(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM);
    StringBuilder sb = new StringBuilder(1024);
    if (null != tmpjars) {
      sb.append(tmpjars);
      sb.append(",");
    }
    sb.append(StringUtils.arrayToString(localUrls.toArray(new String[0])));
    conf.set(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM, sb.toString());
  }

  /**
   * Add the .jar elements of a directory to the DCache classpath, optionally
   * recursively.
   */
  private static void addDirToCache(File dir, FileSystem fs,
    Set<String> localUrls, boolean recursive) {
    if (dir == null) {
      return;
    }

    File[] fileList = dir.listFiles();

    if (fileList == null) {
      LOG.warn("No files under " + dir
        + " to add to distributed cache for hcatalog job");
      return;
    }

    for (File libFile : dir.listFiles()) {
      if (libFile.exists() && !libFile.isDirectory()
        && libFile.getName().endsWith("jar")) {
        Path p = new Path(libFile.toString());
        if (libFile.canRead()) {
          String qualified = p.makeQualified(fs).toString();
          LOG.info("Adding to job classpath: " + qualified);
          localUrls.add(qualified);
        } else {
          LOG.warn("Ignoring unreadable file " + libFile);
        }
      }
      if (recursive && libFile.isDirectory()) {
        addDirToCache(libFile, fs, localUrls, recursive);
      }
    }
  }

  public static boolean isHadoop1() {
    String version = org.apache.hadoop.util.VersionInfo.getVersion();
    if (version.matches("\\b0\\.20\\..+\\b")
      || version.matches("\\b1\\.\\d\\.\\d")) {
      return true;
    }
    return false;
  }
  /**
  * Resolve a database-specific type to HCat data type. Largely follows Sqoop's
  * hive translation.
  * @param sqlType
  *          sql type
  * @return hcat type
  */
 public static String toHCatType(int sqlType) {
   switch (sqlType) {

   // Ideally TINYINT and SMALLINT should be mapped to their
   // HCat equivalents tinyint and smallint respectively
   // But the Sqoop Java type conversion has them mapped to Integer
   // Even though the referenced Java doc clearly recommends otherwise.
   // Changing this now can cause many of the sequence file usages to
   // break as value class implementations will change. So, we
   // just use the same behavior here.
     case Types.SMALLINT:
     case Types.TINYINT:
     case Types.INTEGER:
       return "int";

     case Types.VARCHAR:
       return "varchar";
     case Types.CHAR:
       return "char";
     case Types.LONGVARCHAR:
     case Types.NVARCHAR:
     case Types.NCHAR:
     case Types.LONGNVARCHAR:
     case Types.DATE:
     case Types.TIME:
     case Types.TIMESTAMP:
     case Types.CLOB:
       return "string";

     case Types.FLOAT:
     case Types.REAL:
       return "float";

     case Types.NUMERIC:
     case Types.DECIMAL:
       return "decimal";

     case Types.DOUBLE:
       return "double";

     case Types.BIT:
     case Types.BOOLEAN:
       return "boolean";

     case Types.BIGINT:
       return "bigint";

     case Types.BINARY:
     case Types.VARBINARY:
     case Types.BLOB:
     case Types.LONGVARBINARY:
       return "binary";

     default:
       throw new IllegalArgumentException(
         "Cannot convert SQL type to HCatalog type " + sqlType);
   }
 }


  public void invokeOutputCommitterForLocalMode(Job job) throws IOException {
    if (ConfigurationHelper.isLocalJobTracker(job.getConfiguration()) && isHadoop1()) {
      // HCatalog 0.11- do have special class HCatHadoopShims, however this
      // class got merged into Hive Shim layer in 0.12+. Following method will
      // try to find correct implementation via reflection.
      HadoopShims shims = ShimLoader.getHadoopShims();
      HCatHadoopShims hcatShims = shims.getHCatShim();
      try {
        hcatShims.commitJob(new HCatOutputFormat(), job);
      } catch (Exception e) {
        throw new RuntimeException("Can't explicitly commit job", e);
      }
    }
  }

  public void launchHCatCli(String cmdLine)
    throws IOException {
    String tmpFileName = null;


    String tmpDir = System.getProperty("java.io.tmpdir");
    if (options != null) {
      tmpDir = options.getTempDir();
    }
    tmpFileName =
      new File(tmpDir, "hcat-script-"
        + System.currentTimeMillis()).getAbsolutePath();

    writeHCatScriptFile(tmpFileName, cmdLine);
    // Create the argv for the HCatalog Cli Driver.
    String[] argArray = new String[2];
    argArray[0] = "-f";
    argArray[1] = tmpFileName;
    String argLine = StringUtils.join(",", Arrays.asList(argArray));

    if (testMode) {
      LOG.debug("Executing HCatalog CLI in-process with " + argLine);
      executeHCatProgramInProcess(argArray);
    } else {
      LOG.info("Executing external HCatalog CLI process with args :" + argLine);
      executeExternalHCatProgram(Executor.getCurEnvpStrings(), argArray);
    }
  }

  public void writeHCatScriptFile(String fileName, String contents)
    throws IOException {
    BufferedWriter w = null;
    try {
      FileOutputStream fos = new FileOutputStream(fileName);
      w = new BufferedWriter(new OutputStreamWriter(fos));
      w.write(contents, 0, contents.length());
    } catch (IOException ioe) {
      LOG.error("Error writing HCatalog load-in script", ioe);
      throw ioe;
    } finally {
      if (null != w) {
        try {
          w.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing stream to HCatalog script", ioe);
        }
      }
    }
  }

  /**
   * Execute HCat via an external 'bin/hcat' process.
   * @param env
   *          the environment strings to pass to any subprocess.
   * @throws IOException
   *           if HCatalog did not exit successfully.
   */
  public void executeExternalHCatProgram(List<String> env, String[] cmdLine)
    throws IOException {
    // run HCat command with the given args
    String hCatProgram = getHCatPath();
    ArrayList<String> args = new ArrayList<String>();
    if (Shell.WINDOWS) {
      // windows depends on python to be available
      args.add("python");
    }
    args.add(hCatProgram);
    if (cmdLine != null && cmdLine.length > 0) {
      for (String s : cmdLine) {
        args.add(s);
      }
    }
    LoggingAsyncSink logSink = new LoggingAsyncSink(LOG);
    int ret = Executor.exec(args.toArray(new String[0]),
      env.toArray(new String[0]), logSink, logSink);
    if (0 != ret) {
      throw new IOException("HCat exited with status " + ret);
    }
  }

  public void executeHCatProgramInProcess(String[] argv) throws IOException {
    SubprocessSecurityManager subprocessSM = null;

    try {
      Class<?> cliDriverClass = Class.forName(HCAT_CLI_MAIN_CLASS);
      subprocessSM = new SubprocessSecurityManager();
      subprocessSM.install();
      Method mainMethod = cliDriverClass.getMethod("main", argv.getClass());
      mainMethod.invoke(null, (Object) argv);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("HCatalog class not found", cnfe);
    } catch (NoSuchMethodException nsme) {
      throw new IOException("Could not access HCatCli.main()", nsme);
    } catch (IllegalAccessException iae) {
      throw new IOException("Could not access HatCli.main()", iae);
    } catch (InvocationTargetException ite) {
      // This may have been the ExitSecurityException triggered by the
      // SubprocessSecurityManager.
      Throwable cause = ite.getCause();
      if (cause instanceof ExitSecurityException) {
        ExitSecurityException ese = (ExitSecurityException) cause;
        int status = ese.getExitStatus();
        if (status != 0) {
          throw new IOException("HCatCli  exited with status=" + status);
        }
      } else {
        throw new IOException("Exception thrown from HCatCli", ite);
      }
    } finally {
      if (null != subprocessSM) {
        subprocessSM.uninstall();
      }
    }
  }

  /**
   * @return the filename of the hcat executable to run to do the import
   */
  public String getHCatPath() {
    String hCatHome = null;
    if (options == null) {
      hCatHome = SqoopOptions.getHCatHomeDefault();
    } else {
      hCatHome = options.getHCatHome();
    }

    if (null == hCatHome) {
      return null;
    }

    Path p = new Path(hCatHome);
    p = new Path(p, "bin");
    p = new Path(p, HCATCMD);
    String hCatBinStr = p.toString();
    if (new File(hCatBinStr).canExecute()) {
      return hCatBinStr;
    } else {
      return null;
    }
  }

  public static boolean isTestMode() {
    return testMode;
  }

  public static void setTestMode(boolean mode) {
    testMode = mode;
  }

  public static Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  public static Class<? extends OutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  public static void setInputFormatClass(Class<? extends InputFormat> clz) {
    inputFormatClass = clz;
  }

  public static void setOutputFormatClass(Class<? extends OutputFormat> clz) {
    outputFormatClass = clz;
  }

  public static Class<? extends Mapper> getImportMapperClass() {
    return importMapperClass;
  }

  public static Class<? extends Mapper> getExportMapperClass() {
    return exportMapperClass;
  }

  public static void setExportMapperClass(Class<? extends Mapper> clz) {
    exportMapperClass = clz;
  }

  public static void setImportMapperClass(Class<? extends Mapper> clz) {
    importMapperClass = clz;
  }

  public static Class<? extends Writable> getImportValueClass() {
    return importValueClass;
  }

  public static void setImportValueClass(Class<? extends Writable> clz) {
    importValueClass = clz;
  }


  public String getDatabaseTable() {
    return dbTableName;
  }

  public String getHCatTableName() {
    return hCatTableName;
  }

  public String getHCatDatabaseName() {
    return hCatDatabaseName;
  }

  public String getQualifiedHCatTableName() {
    return hCatQualifiedTableName;
  }

  public List<String> getHCatDynamicPartitionKeys() {
    return hCatDynamicPartitionKeys;
  }

  public List<String> getHCatStaticPartitionKey() {
    return hCatStaticPartitionKeys;
  }

  public String[] getDBColumnNames() {
    return dbColumnNames;
  }

  public HCatSchema getHCatOutputSchema() {
    return hCatOutputSchema;
  }

  public void setHCatOutputSchema(HCatSchema schema) {
    hCatOutputSchema = schema;
  }

  public HCatSchema getHCatPartitionSchema() {
    return hCatPartitionSchema;
  }

  public void setHCatPartitionSchema(HCatSchema schema) {
    hCatPartitionSchema = schema;
  }

  public void setHCatStaticPartitionKey(List<String> keys) {
    hCatStaticPartitionKeys = keys;
  }

  public void setHCatDynamicPartitionKeys(List<String> keys) {
    hCatDynamicPartitionKeys = keys;
  }

  public String[] getDbColumnNames() {
    return dbColumnNames;
  }

  public void setDbColumnNames(String[] names) {
    dbColumnNames = names;
  }

  public Map<String, List<Integer>> getDbColumnInfo() {
    return dbColumnInfo;
  }

  public String gethCatTableName() {
    return hCatTableName;
  }

  public String gethCatDatabaseName() {
    return hCatDatabaseName;
  }

  public String gethCatQualifiedTableName() {
    return hCatQualifiedTableName;
  }

  public void setConfigured(boolean value) {
    configured = value;
  }

  public static String sqlTypeString(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
        return "BIT";
      case Types.TINYINT:
        return "TINYINT";
      case Types.SMALLINT:
        return "SMALLINT";
      case Types.INTEGER:
        return "INTEGER";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.REAL:
        return "REAL";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.NUMERIC:
        return "NUMERIC";
      case Types.DECIMAL:
        return "DECIMAL";
      case Types.CHAR:
        return "CHAR";
      case Types.VARCHAR:
        return "VARCHAR";
      case Types.LONGVARCHAR:
        return "LONGVARCHAR";
      case Types.DATE:
        return "DATE";
      case Types.TIME:
        return "TIME";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      case Types.BINARY:
        return "BINARY";
      case Types.VARBINARY:
        return "VARBINARY";
      case Types.LONGVARBINARY:
        return "LONGVARBINARY";
      case Types.NULL:
        return "NULL";
      case Types.OTHER:
        return "OTHER";
      case Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
      case Types.DISTINCT:
        return "DISTINCT";
      case Types.STRUCT:
        return "STRUCT";
      case Types.ARRAY:
        return "ARRAY";
      case Types.BLOB:
        return "BLOB";
      case Types.CLOB:
        return "CLOB";
      case Types.REF:
        return "REF";
      case Types.DATALINK:
        return "DATALINK";
      case Types.BOOLEAN:
        return "BOOLEAN";
      case Types.ROWID:
        return "ROWID";
      case Types.NCHAR:
        return "NCHAR";
      case Types.NVARCHAR:
        return "NVARCHAR";
      case Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
      case Types.NCLOB:
        return "NCLOB";
      case Types.SQLXML:
        return "SQLXML";
      default:
        return "<UNKNOWN>";
    }
  }
}
