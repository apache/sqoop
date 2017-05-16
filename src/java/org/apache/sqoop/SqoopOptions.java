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

package org.apache.sqoop;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.accumulo.AccumuloConstants;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;
import org.apache.sqoop.tool.BaseSqoopTool;
import org.apache.sqoop.util.CredentialsUtil;
import org.apache.sqoop.util.LoggingUtils;
import org.apache.sqoop.util.SqoopJsonUtil;
import org.apache.sqoop.util.password.CredentialProviderHelper;
import org.apache.sqoop.validation.AbortOnFailureHandler;
import org.apache.sqoop.validation.AbsoluteValidationThreshold;
import org.apache.sqoop.validation.RowCountValidator;

import com.cloudera.sqoop.SqoopOptions.FileLayout;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.SqoopOptions.UpdateMode;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.tool.SqoopTool;
import com.cloudera.sqoop.util.RandomHash;
import com.cloudera.sqoop.util.StoredAsProperty;

import static org.apache.sqoop.Sqoop.SQOOP_RETHROW_PROPERTY;
import static org.apache.sqoop.orm.ClassWriter.toJavaIdentifier;

/**
 * Configurable state used by Sqoop tools.
 */
public class SqoopOptions implements Cloneable {

  public static final String ORACLE_ESCAPING_DISABLED = "sqoop.oracle.escaping.disabled";

  private static final String OLD_SQOOP_TEST_IMPORT_ROOT_DIR = "sqoop.test.import.rootDir";

  public static final Log LOG = LogFactory.getLog(SqoopOptions.class.getName());

  /**
   * Set to true in configuration if you want to put db passwords
   * in the metastore.
   */
  public static final String METASTORE_PASSWORD_KEY =
      "sqoop.metastore.client.record.password";

  // Default hive and hcat locations.
  public static final String DEF_HIVE_HOME = "/usr/lib/hive";
  public static final String DEF_HCAT_HOME = "/usr/lib/hive-hcatalog";
  public static final String DEF_HCAT_HOME_OLD = "/usr/lib/hcatalog";

  public static final boolean METASTORE_PASSWORD_DEFAULT = false;
  /**
   * Thrown when invalid cmdline options are given.
   */
  @SuppressWarnings("serial")
  public static class InvalidOptionsException extends Exception {

    private String message;

    public InvalidOptionsException(final String msg) {
      this.message = msg;
    }

    public String getMessage() {
      return message;
    }

    public String toString() {
      return getMessage();
    }
  }

  @StoredAsProperty("customtool.options.jsonmap")
  private Map<String, String> customToolOptions;

  // TODO(aaron): Adding something here? Add a setter and a getter.  Add a
  // default value in initDefaults() if you need one.  If this value needs to
  // be serialized in the metastore, it should be marked with
  // @StoredAsProperty(), if it is an int, long, boolean, String, or Enum.
  // Arrays and other "special" types should be added directly to the
  // loadProperties() and writeProperties() methods. Then add command-line
  // arguments in the appropriate tools. The names of all command-line args
  // are stored as constants in BaseSqoopTool.

  @StoredAsProperty("verbose") private boolean verbose;

  @StoredAsProperty("temporary.dirRoot") private String tempRootDir;

  // If this property is set, always throw an exception during a job, do not just
  // exit with status 1.
  @StoredAsProperty("sqoop.throwOnError") private boolean throwOnError;

  @StoredAsProperty("mapreduce.job.name") private String mapreduceJobName;

  @StoredAsProperty("db.connect.string") private String connectString;
  @StoredAsProperty("db.table") private String tableName;
  private String [] columns; // Array stored as db.column.list.
  @StoredAsProperty("db.username") private String username;
  @StoredAsProperty("db.export.staging.table") private String stagingTableName;
  @StoredAsProperty("db.clear.staging.table") private boolean clearStagingTable;
  @StoredAsProperty("db.export.call") private String call;
  private Properties connectionParams; //Properties stored as db.connect.params


  // May not be serialized, based on configuration.
  // db.require.password is used to determine whether 'some' password is
  // used. If so, it is stored as 'db.password'.
  private String password;

  // This represents path to a file on ${user.home} containing the password
  // with 400 permissions so its only readable by user executing the tool
  @StoredAsProperty("db.password.file") private String passwordFilePath;
  @StoredAsProperty("db.password.alias") private String passwordAlias;

  @StoredAsProperty("null.string") private String nullStringValue;
  @StoredAsProperty("input.null.string") private String inNullStringValue;
  @StoredAsProperty("null.non-string") private String nullNonStringValue;
  @StoredAsProperty("input.null.non-string")
      private String inNullNonStringValue;

  @StoredAsProperty("codegen.output.dir") private String codeOutputDir;
  @StoredAsProperty("codegen.compile.dir") private String jarOutputDir;
  // Boolean specifying whether jarOutputDir is a nonce tmpdir (true), or
  // explicitly set by the user (false). If the former, disregard any value
  // for jarOutputDir saved in the metastore.
  @StoredAsProperty("codegen.auto.compile.dir") private boolean jarDirIsAuto;
  private String hadoopMapRedHome; // not serialized to metastore.
  @StoredAsProperty("db.split.column") private String splitByCol;
  @StoredAsProperty("split.limit") private Integer splitLimit;
  @StoredAsProperty("db.where.clause") private String whereClause;
  @StoredAsProperty("db.query") private String sqlQuery;
  @StoredAsProperty("db.query.boundary") private String boundaryQuery;
  @StoredAsProperty("jdbc.driver.class") private String driverClassName;
  @StoredAsProperty("hdfs.warehouse.dir") private String warehouseDir;
  @StoredAsProperty("hdfs.target.dir") private String targetDir;
  @StoredAsProperty("hdfs.append.dir") private boolean append;
  @StoredAsProperty("hdfs.delete-target.dir") private boolean delete;
  @StoredAsProperty("hdfs.file.format") private FileLayout layout;
  @StoredAsProperty("direct.import") private boolean direct; // "direct mode."
  @StoredAsProperty("db.batch") private boolean batchMode;
  private String tmpDir; // where temp data goes; usually /tmp; not serialized.
  private String hiveHome; // not serialized to metastore.
  @StoredAsProperty("hive.import") private boolean hiveImport;
  @StoredAsProperty("hive.overwrite.table") private boolean overwriteHiveTable;
  @StoredAsProperty("hive.fail.table.exists")
  private boolean failIfHiveTableExists;
  @StoredAsProperty("hive.external.table.dir") private String hiveExternalTableDir;
  @StoredAsProperty("hive.table.name") private String hiveTableName;
  @StoredAsProperty("hive.database.name") private String hiveDatabaseName;
  @StoredAsProperty("hive.drop.delims") private boolean hiveDropDelims;
  @StoredAsProperty("hive.delims.replacement")
  private String hiveDelimsReplacement;
  @StoredAsProperty("hive.partition.key") private String hivePartitionKey;
  @StoredAsProperty("hive.partition.value") private String hivePartitionValue;
  @StoredAsProperty("hcatalog.table.name")
  private String hCatTableName;
  @StoredAsProperty("hcatalog.database.name")
  private String hCatDatabaseName;
  @StoredAsProperty("hcatalog.create.table")
  private boolean hCatCreateTable;
  @StoredAsProperty("hcatalog.drop.and.create.table")
  private boolean hCatDropAndCreateTable;
  @StoredAsProperty("hcatalog.storage.stanza")
  private String hCatStorageStanza;
  private String hCatHome; // not serialized to metastore.
  @StoredAsProperty("skip.dist.cache")
  private boolean skipDistCache;
  @StoredAsProperty("hcatalog.partition.keys")
    private String hCatalogPartitionKeys;
  @StoredAsProperty("hcatalog.partition.values")
    private String hCatalogPartitionValues;
  // User explicit mapping of types
  private Properties mapColumnJava; // stored as map.colum.java
  private Properties mapColumnHive; // stored as map.column.hive
  // SQOOP-3123 default enabled
  private boolean escapeColumnMappingEnabled;
  private Properties mapReplacedColumnJava; // used to replace special characters in columns

  // An ordered list of column names denoting what order columns are
  // serialized to a PreparedStatement from a generated record type.
  // Not serialized to metastore.
  private String [] dbOutColumns;

  // package to prepend to auto-named classes.
  @StoredAsProperty("codegen.java.packagename") private String packageName;

  // package+class to apply to individual table import.
  // also used as an *input* class with existingJarFile.
  @StoredAsProperty("codegen.java.classname") private String className;

  // Name of a jar containing existing table definition
  // class to use.
  @StoredAsProperty("codegen.jar.file") private String existingJarFile;

  @StoredAsProperty("mapreduce.num.mappers") private int numMappers;
  @StoredAsProperty("enable.compression") private boolean useCompression;
  @StoredAsProperty("compression.codec") private String compressionCodec;

  // In direct mode, open a new stream every X bytes.
  @StoredAsProperty("import.direct.split.size") private long directSplitSize;

  // Max size of an inline LOB; larger LOBs are written
  // to external files on disk.
  @StoredAsProperty("import.max.inline.lob.size") private long maxInlineLobSize;

  // Max number 'n' of rows to fetch from the
  // database when more rows are needed.
  @StoredAsProperty("import.fetch.size") private Integer fetchSize;

  // HDFS path to read from when performing an export
  @StoredAsProperty("export.source.dir") private String exportDir;

  // Column to use for the WHERE clause in an UPDATE-based export.
  @StoredAsProperty("export.update.col") private String updateKeyCol;

  @StoredAsProperty("export.new.update") private UpdateMode updateMode;

  private DelimiterSet inputDelimiters; // codegen.input.delimiters.
  private DelimiterSet outputDelimiters; // codegen.output.delimiters.

  private boolean areOutputDelimsManuallySet;
  private boolean areInputDelimsManuallySet;

  private Configuration conf;
  private String toolName;

  public static final int DEFAULT_NUM_MAPPERS = 4;

  private String [] extraArgs;

  // HBase table to import into.
  @StoredAsProperty("hbase.table") private String hbaseTable;

  // Column family to prepend to inserted cols.
  @StoredAsProperty("hbase.col.family") private String hbaseColFamily;

  // Column of the input to use as the row key.
  @StoredAsProperty("hbase.row.key.col") private String hbaseRowKeyCol;

  // if true, bulk loading will be used.
  @StoredAsProperty("hbase.bulk.load.enabled") private boolean hbaseBulkLoadEnabled;

  // if true, create tables/col families.
  @StoredAsProperty("hbase.create.table") private boolean hbaseCreateTable;

  // col to filter on for incremental imports.
  @StoredAsProperty("incremental.col") private String incrementalTestCol;
  // incremental import mode we're using.
  @StoredAsProperty("incremental.mode")
  private IncrementalMode incrementalMode;
  // What was the last-imported value of incrementalTestCol?
  @StoredAsProperty("incremental.last.value")
  private String incrementalLastValue;

  // exclude these tables when importing all tables.
  @StoredAsProperty("import.all_tables.exclude")
  private String allTablesExclude;

  // HDFS paths for "old" and "new" datasets in merge tool.
  @StoredAsProperty("merge.old.path") private String mergeOldPath;
  @StoredAsProperty("merge.new.path") private String mergeNewPath;

  // "key" column for the merge operation.
  @StoredAsProperty("merge.key.col") private String mergeKeyCol;

  // Dataset name for mainframe import tool
  @StoredAsProperty("mainframe.input.dataset.name")
  private String mainframeInputDatasetName;

  // Dataset type for mainframe import tool
  @StoredAsProperty("mainframe.input.dataset.type")
  private String mainframeInputDatasetType;

  // Indicates if the data set is on tape to use different FTP parser
  @StoredAsProperty("mainframe.input.dataset.tape")
  private String mainframeInputDatasetTape;

  // Accumulo home directory
  private String accumuloHome; // not serialized to metastore.
  // Zookeeper home directory
  private String zookeeperHome; // not serialized to metastore.

  // Accumulo table to import into.
  @StoredAsProperty("accumulo.table") private String accumuloTable;

  // Column family to prepend to inserted cols.
  @StoredAsProperty("accumulo.col.family") private String accumuloColFamily;

  // Column of the input to use as the row key.
  @StoredAsProperty("accumulo.row.key.col") private String accumuloRowKeyCol;
  //
  // Visibility token to be applied to each row imported.
  @StoredAsProperty("accumulo.visibility") private String accumuloVisibility;

  // Size of the write buffer.
  @StoredAsProperty("accumulo.batch.size")
  private long accumuloBatchSize;

  // Maximum latency for batch writer.
  @StoredAsProperty("accumulo.max.latency")
  private long accumuloMaxLatency;

  // if true, create table.
  @StoredAsProperty("accumulo.create.table")
  private boolean accumuloCreateTable;

  // Accumulo user name
  @StoredAsProperty("accumulo.user") private String accumuloUser;

  // Accumulo password
  @StoredAsProperty("accumulo.password") private String accumuloPassword;

  // Accumulo instance
  @StoredAsProperty("accumulo.instance") private String accumuloInstance;

  // Accumulo zookeeper
  @StoredAsProperty("accumulo.zookeepers") private String accumuloZookeepers;

  // Relaxed Isolation
  @StoredAsProperty("relaxed.isolation") private boolean relaxedIsolation;

  // Use single mapper for non-primary key tables without
  // explicit split by cols
  @StoredAsProperty("reset.onemapper") private boolean autoResetToOneMapper;

  @StoredAsProperty("sqlconnection.metadata.transaction.isolation.level") private int metadataTransactionIsolationLevel;

  // These next two fields are not serialized to the metastore.
  // If this SqoopOptions is created by reading a saved job, these will
  // be populated by the JobStorage to facilitate updating the same
  // job.
  private String jobName;
  private Map<String, String> jobStorageDescriptor;

  // If we restore a job and then allow the user to apply arguments on
  // top, we retain the version without the arguments in a reference to the
  // 'parent' SqoopOptions instance, here.
  private com.cloudera.sqoop.SqoopOptions parent;

  // Nonce directory name. Generate one per process, lazily, if
  // getNonceJarDir() is called. Not recorded in metadata. This is used as
  // a temporary holding area for compilation work done by this process.
  private static String curNonce;

  // the connection manager fully qualified class name
  @StoredAsProperty("connection.manager") private String connManagerClassName;

  // The currently active tool. (Not saved in properties)
  // Used to pass the SqoopTool instance in to mapreduce job configuration
  // (JobBase, etc).
  private SqoopTool activeSqoopTool;

  // Flag to determine if data copied needs to be validated against the source
  private boolean isValidationEnabled;
  // These take FQCN as input, convert them to Class in light of failing early
  private Class validatorClass; // Class for the validator implementation.
  private Class validationThresholdClass; // ValidationThreshold implementation
  private Class validationFailureHandlerClass; // FailureHandler implementation

  @StoredAsProperty(ORACLE_ESCAPING_DISABLED)
  private boolean oracleEscapingDisabled;

  public SqoopOptions() {
    initDefaults(null);
  }

  public SqoopOptions(Configuration conf) {
    initDefaults(conf);
  }

  /**
   * Alternate SqoopOptions interface used mostly for unit testing.
   * @param connect JDBC connect string to use
   * @param table Table to read
   */
  public SqoopOptions(final String connect, final String table) {
    initDefaults(null);

    this.connectString = connect;
    this.tableName = table;
  }

  private boolean getBooleanProperty(Properties props, String propName,
      boolean defaultValue) {
    String str = props.getProperty(propName,
        Boolean.toString(defaultValue)).toLowerCase();
    return "true".equals(str) || "yes".equals(str) || "1".equals(str);
  }

  private long getLongProperty(Properties props, String propName,
      long defaultValue) {
    String str = props.getProperty(propName,
        Long.toString(defaultValue)).toLowerCase();
    try {
      return Long.parseLong(str);
    } catch (NumberFormatException nfe) {
      LOG.warn("Could not parse integer value for config parameter "
          + propName);
      return defaultValue;
    }
  }

  private int getIntProperty(Properties props, String propName,
      int defaultVal) {
    long longVal = getLongProperty(props, propName, defaultVal);
    return (int) longVal;
  }

  private char getCharProperty(Properties props, String propName,
      char defaultVal) {
    int intVal = getIntProperty(props, propName, (int) defaultVal);
    return (char) intVal;
  }

  private DelimiterSet getDelimiterProperties(Properties props,
      String prefix, DelimiterSet defaults) {

    if (null == defaults) {
      defaults = new DelimiterSet();
    }

    char field = getCharProperty(props, prefix + ".field",
        defaults.getFieldsTerminatedBy());
    char record = getCharProperty(props, prefix + ".record",
        defaults.getLinesTerminatedBy());
    char enclose = getCharProperty(props, prefix + ".enclose",
        defaults.getEnclosedBy());
    char escape = getCharProperty(props, prefix + ".escape",
        defaults.getEscapedBy());
    boolean required = getBooleanProperty(props, prefix +".enclose.required",
        defaults.isEncloseRequired());

    return new DelimiterSet(field, record, enclose, escape, required);
  }

  private void setDelimiterProperties(Properties props,
      String prefix, DelimiterSet values) {
    putProperty(props, prefix + ".field",
        Integer.toString((int) values.getFieldsTerminatedBy()));
    putProperty(props, prefix + ".record",
        Integer.toString((int) values.getLinesTerminatedBy()));
    putProperty(props, prefix + ".enclose",
        Integer.toString((int) values.getEnclosedBy()));
    putProperty(props, prefix + ".escape",
        Integer.toString((int) values.getEscapedBy()));
    putProperty(props, prefix + ".enclose.required",
        Boolean.toString(values.isEncloseRequired()));
  }

  /** Take a comma-delimited list of input and split the elements
   * into an output array. */
  private String [] listToArray(String strList) {
    return strList.split(",");
  }

  private String arrayToList(String [] array) {
    if (null == array) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String elem : array) {
      if (!first) {
        sb.append(",");
      }
      sb.append(elem);
      first = false;
    }

    return sb.toString();
  }

  /**
   * A put() method for Properties that is tolerent of 'null' values.
   * If a null value is specified, the property is unset.
   */
  private void putProperty(Properties props, String k, String v) {
    if (null == v) {
      props.remove(k);
    } else {
      props.setProperty(k, v);
    }
  }

  /**
   * Given a property prefix that denotes a set of numbered properties,
   * return an array containing all the properties.
   *
   * For instance, if prefix is "foo", then return properties "foo.0",
   * "foo.1", "foo.2", and so on as an array. If no such properties
   * exist, return 'defaults'.
   */
  private String [] getArgArrayProperty(Properties props, String prefix,
      String [] defaults) {
    int cur = 0;
    ArrayList<String> al = new ArrayList<String>();
    while (true) {
      String curProp = prefix + "." + cur;
      String curStr = props.getProperty(curProp, null);
      if (null == curStr) {
        break;
      }

      al.add(curStr);
      cur++;
    }

    if (cur == 0) {
      // Couldn't find an array here; return the defaults.
      return defaults;
    }

    return al.toArray(new String[0]);
  }

  private void setArgArrayProperties(Properties props, String prefix,
      String [] values) {
    if (null == values) {
      return;
    }

    for (int i = 0; i < values.length; i++) {
      putProperty(props, prefix + "." + i, values[i]);
    }
  }

  /**
   * This method encodes the property key values found in the provided
   * properties instance <tt>values</tt> into another properties instance
   * <tt>props</tt>. The specified <tt>prefix</tt> is used as a namespace
   * qualifier for keys when inserting. This allows easy introspection of the
   * property key values in <tt>props</tt> instance to later separate out all
   * the properties that belong to the <tt>values</tt> instance.
   * @param props the container properties instance
   * @param prefix the prefix for qualifying contained property keys.
   * @param values the contained properties instance, all of whose elements will
   *               be added to the container properties instance.
   *
   * @see #getPropertiesAsNetstedProperties(Properties, String)
   */
  private void setPropertiesAsNestedProperties(Properties props,
          String prefix, Properties values) {
    String nestedPropertyPrefix = prefix + ".";
    if (null == values || values.size() == 0) {
      Iterator<String> it = props.stringPropertyNames().iterator();
      while (it.hasNext()) {
        String name = it.next();
        if (name.startsWith(nestedPropertyPrefix)) {
          props.remove(name);
        }
      }
    } else {
      Iterator<String> it = values.stringPropertyNames().iterator();
      while (it.hasNext()) {
        String name = it.next();
        putProperty(props,
                nestedPropertyPrefix + name, values.getProperty(name));
      }
    }
  }

  /**
   * This method decodes the property key values found in the provided
   * properties instance <tt>props</tt> that have keys beginning with the
   * given prefix. Matching elements from this properties instance are modified
   * so that their prefix is dropped.
   * @param props the properties container
   * @param prefix the prefix qualifying properties that need to be removed
   * @return a new properties instance that contains all matching elements from
   * the container properties.
   */
  private Properties getPropertiesAsNetstedProperties(
          Properties props, String prefix) {
    Properties nestedProps = new Properties();
    String nestedPropertyPrefix = prefix + ".";
    int index = nestedPropertyPrefix.length();
    if (props != null && props.size() > 0) {
      Iterator<String> it = props.stringPropertyNames().iterator();
      while (it.hasNext()) {
        String name = it.next();
        if (name.startsWith(nestedPropertyPrefix)){
          String shortName = name.substring(index);
          nestedProps.put(shortName, props.get(name));
        }
      }
    }
    return nestedProps;
  }

  @SuppressWarnings("unchecked")
  /**
   * Given a set of properties, load this into the current SqoopOptions
   * instance.
   */
  public void loadProperties(Properties props) {

    try {
      Field [] fields = SqoopOptions.class.getDeclaredFields();
      for (Field f : fields) {
        if (f.isAnnotationPresent(StoredAsProperty.class)) {
          Class typ = f.getType();
          StoredAsProperty storedAs = f.getAnnotation(StoredAsProperty.class);
          String propName = storedAs.value();

          if (typ.equals(int.class)) {
            f.setInt(this,
                getIntProperty(props, propName, f.getInt(this)));
          } else if (typ.equals(boolean.class)) {
            f.setBoolean(this,
                getBooleanProperty(props, propName, f.getBoolean(this)));
          } else if (typ.equals(long.class)) {
            f.setLong(this,
                getLongProperty(props, propName, f.getLong(this)));
          } else if (typ.equals(String.class)) {
            f.set(this, props.getProperty(propName, (String) f.get(this)));
          } else if (typ.equals(Integer.class)) {
            String value = props.getProperty(
                propName,
                f.get(this) == null ? "null" : f.get(this).toString());
            f.set(this, value.equals("null") ? null : new Integer(value));
          } else if (typ.isEnum()) {
            f.set(this, Enum.valueOf(typ,
                props.getProperty(propName, f.get(this).toString())));
          }  else if (typ.equals(Map.class)) {
            f.set(this,
                SqoopJsonUtil.getMapforJsonString(props.getProperty(propName)));
          } else {
            throw new RuntimeException("Could not retrieve property "
                + propName + " for type: " + typ);
          }
        }
      }
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Illegal access to field in property setter",
          iae);
    }

    // Now load properties that were stored with special types, or require
    // additional logic to set.

    loadPasswordProperty(props);

    if (this.jarDirIsAuto) {
      // We memoized a user-specific nonce dir for compilation to the data
      // store.  Disregard that setting and create a new nonce dir.
      String localUsername = System.getProperty("user.name", "unknown");
      this.jarOutputDir = getNonceJarDir(tmpDir + "sqoop-" + localUsername
          + "/compile");
    }

    String colListStr = props.getProperty("db.column.list", null);
    if (null != colListStr) {
      this.columns = listToArray(colListStr);
    }

    this.inputDelimiters = getDelimiterProperties(props,
        "codegen.input.delimiters", this.inputDelimiters);
    this.outputDelimiters = getDelimiterProperties(props,
        "codegen.output.delimiters", this.outputDelimiters);

    this.extraArgs = getArgArrayProperty(props, "tool.arguments",
        this.extraArgs);

    this.connectionParams =
        getPropertiesAsNetstedProperties(props, "db.connect.params");

    // Loading user mapping
    this.mapColumnHive =
            getPropertiesAsNetstedProperties(props, "map.column.hive");
    this.mapColumnJava =
            getPropertiesAsNetstedProperties(props, "map.column.java");

    // Delimiters were previously memoized; don't let the tool override
    // them with defaults.
    this.areOutputDelimsManuallySet = true;
    this.areInputDelimsManuallySet = true;

    // If we loaded true verbose flag, we need to apply it
    if (this.verbose) {
      LoggingUtils.setDebugLevel();
    }

    // Ensuring that oracleEscapingDisabled property is propagated to
    // the level of Hadoop configuration as well
    this.setOracleEscapingDisabled(this.isOracleEscapingDisabled());
  }

  private void loadPasswordProperty(Properties props) {
    passwordFilePath = props.getProperty("db.password.file");
    if (passwordFilePath != null) {
      try {
        password = CredentialsUtil.fetchPasswordFromLoader(passwordFilePath, getConf());
        return; // short-circuit
      } catch (IOException e) {
        throw new RuntimeException("Unable to fetch password from file.", e);
      }
    }

    passwordAlias = props.getProperty("db.password.alias");
    if (passwordAlias != null) {
      try {
        setPassword(CredentialProviderHelper.resolveAlias(getConf(), passwordAlias));
        return; // short-circuit
      } catch (IOException e) {
        throw new RuntimeException("Unable to resolve credentials.", e);
      }
    }

    if (getBooleanProperty(props, "db.require.password", false)) {
      // The user's password was stripped out from the metastore.
      // Require that the user enter it now.
      setPasswordFromConsole();
    } else {
      this.password = props.getProperty("db.password", this.password);
    }
  }

  /**
   * Return a Properties instance that encapsulates all the "sticky"
   * state of this SqoopOptions that should be written to a metastore
   * to restore the job later.
   */
  public Properties writeProperties() {
    Properties props = new Properties();

    try {
      Field [] fields = SqoopOptions.class.getDeclaredFields();
      for (Field f : fields) {
        if (f.isAnnotationPresent(StoredAsProperty.class)) {
          Class typ = f.getType();
          StoredAsProperty storedAs = f.getAnnotation(StoredAsProperty.class);
          String propName = storedAs.value();

          if (typ.equals(int.class)) {
            putProperty(props, propName, Integer.toString(f.getInt(this)));
          } else if (typ.equals(boolean.class)) {
            putProperty(props, propName, Boolean.toString(f.getBoolean(this)));
          } else if (typ.equals(long.class)) {
            putProperty(props, propName, Long.toString(f.getLong(this)));
          } else if (typ.equals(String.class)) {
            putProperty(props, propName, (String) f.get(this));
          } else if (typ.equals(Integer.class)) {
            putProperty(
                props,
                propName,
                f.get(this) == null ? "null" : f.get(this).toString());
          } else if (typ.isEnum()) {
            putProperty(props, propName, f.get(this).toString());
          } else if (typ.equals(Map.class)) {
            putProperty(
                props,
                propName,
                SqoopJsonUtil.getJsonStringforMap((Map) f.get(this)));
          } else {
            throw new RuntimeException("Could not set property "
                + propName + " for type: " + typ);
          }
        }
      }
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Illegal access to field in property setter",
          iae);
    }

    writePasswordProperty(props);

    putProperty(props, "db.column.list", arrayToList(this.columns));
    setDelimiterProperties(props, "codegen.input.delimiters",
        this.inputDelimiters);
    setDelimiterProperties(props, "codegen.output.delimiters",
        this.outputDelimiters);
    setArgArrayProperties(props, "tool.arguments", this.extraArgs);

    setPropertiesAsNestedProperties(props,
            "db.connect.params", this.connectionParams);

    setPropertiesAsNestedProperties(props,
            "map.column.hive", this.mapColumnHive);
    setPropertiesAsNestedProperties(props,
            "map.column.java", this.mapColumnJava);
    return props;
  }

  private void writePasswordProperty(Properties props) {
    if (getPasswordFilePath() != null) { // short-circuit
      putProperty(props, "db.password.file", getPasswordFilePath());
      return;
    }

    if (getPasswordAlias() != null) { // short-circuit
      putProperty(props, "db.password.alias", getPasswordAlias());
      return;
    }

    if (this.getConf().getBoolean(
      METASTORE_PASSWORD_KEY, METASTORE_PASSWORD_DEFAULT)) {
      // If the user specifies, we may store the password in the metastore.
      putProperty(props, "db.password", this.password);
      putProperty(props, "db.require.password", "false");
    } else if (this.password != null) {
      // Otherwise, if the user has set a password, we just record
      // a flag stating that the password will need to be reentered.
      putProperty(props, "db.require.password", "true");
    } else {
      // No password saved or required.
      putProperty(props, "db.require.password", "false");
    }
  }

  @Override
  public Object clone() {
    try {
      SqoopOptions other = (SqoopOptions) super.clone();
      if (null != columns) {
        other.columns = Arrays.copyOf(columns, columns.length);
      }

      if (null != dbOutColumns) {
        other.dbOutColumns = Arrays.copyOf(dbOutColumns, dbOutColumns.length);
      }

      if (null != inputDelimiters) {
        other.inputDelimiters = (DelimiterSet) inputDelimiters.clone();
      }

      if (null != outputDelimiters) {
        other.outputDelimiters = (DelimiterSet) outputDelimiters.clone();
      }

      if (null != conf) {
        other.conf = new Configuration(conf);
      }

      if (null != extraArgs) {
        other.extraArgs = Arrays.copyOf(extraArgs, extraArgs.length);
      }

      if (null != connectionParams) {
        other.setConnectionParams(this.connectionParams);
      }

      if (null != mapColumnHive) {
        other.mapColumnHive = (Properties) this.mapColumnHive.clone();
      }

      if (null != mapColumnJava) {
        other.mapColumnJava = (Properties) this.mapColumnJava.clone();
      }

      if (null != mapReplacedColumnJava) {
        other.mapReplacedColumnJava = (Properties) this.mapReplacedColumnJava.clone();
      }

      return other;
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen.
      return null;
    }
  }

  /**
   * @return the temp directory to use; this is guaranteed to end with
   * the file separator character (e.g., '/').
   */
  public String getTempDir() {
    return this.tmpDir;
  }

  /**
   * Return the name of a directory that does not exist before
   * calling this method, and does exist afterward. We should be
   * the only client of this directory. If this directory is not
   * used during the lifetime of the JVM, schedule it to be removed
   * when the JVM exits.
   */
  private static String getNonceJarDir(String tmpBase) {

    // Make sure we don't loop forever in the event of a permission error.
    final int MAX_DIR_CREATE_ATTEMPTS = 32;

    if (null != curNonce) {
      return curNonce;
    }

    File baseDir = new File(tmpBase);
    File hashDir = null;

    for (int attempts = 0; attempts < MAX_DIR_CREATE_ATTEMPTS; attempts++) {
      hashDir = new File(baseDir, RandomHash.generateMD5String());
      while (hashDir.exists()) {
        hashDir = new File(baseDir, RandomHash.generateMD5String());
      }

      if (hashDir.mkdirs()) {
        // We created the directory. Use it.
        // If this directory is not actually filled with files, delete it
        // when the JVM quits.
        hashDir.deleteOnExit();
        break;
      }
    }

    if (hashDir == null || !hashDir.exists()) {
      throw new RuntimeException("Could not create temporary directory: "
          + hashDir + "; check for a directory permissions issue on /tmp.");
    }

    LOG.debug("Generated nonce dir: " + hashDir.toString());
    SqoopOptions.curNonce = hashDir.toString();
    return SqoopOptions.curNonce;
  }

  /**
   * Reset the nonce directory and force a new one to be generated. This
   * method is intended to be used only by multiple unit tests that want
   * to isolate themselves from one another. It should not be called
   * during normal Sqoop execution.
   */
  public static void clearNonceDir() {
    LOG.warn("Clearing nonce directory");
    SqoopOptions.curNonce = null;
  }

  public static String getHiveHomeDefault() {
    // Set this with $HIVE_HOME, but -Dhive.home can override.
    String hiveHome = System.getenv("HIVE_HOME");
    hiveHome = System.getProperty("hive.home", hiveHome);
    if (hiveHome == null) {
      hiveHome = DEF_HIVE_HOME;
    }
    return hiveHome;
  }

  public static String getHCatHomeDefault() {
    // Set this with $HCAT_HOME, but -Dhcatalog.home can override.
    String hcatHome = System.getenv("HCAT_HOME");
    hcatHome = System.getProperty("hcat.home", hcatHome);
    if (hcatHome == null) {
      File file = new File(DEF_HCAT_HOME);
      if (file.exists()) {
        hcatHome = DEF_HCAT_HOME;
      } else {
        hcatHome = DEF_HCAT_HOME_OLD;
      }
    }
    return hcatHome;
  }

  private void initDefaults(Configuration baseConfiguration) {
    // first, set the true defaults if nothing else happens.
    // default action is to run the full pipeline.
    this.hadoopMapRedHome = System.getenv("HADOOP_MAPRED_HOME");

    this.accumuloHome = getAccumuloHomeDefault();
    this.zookeeperHome = getZookeeperHomeDefault();
    this.hiveHome = getHiveHomeDefault();
    this.hCatHome = getHCatHomeDefault();

    this.inputDelimiters = new DelimiterSet(
        DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR,
        DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, false);
    this.outputDelimiters = new DelimiterSet();

    // Set this to cwd, but -Dsqoop.src.dir can override.
    this.codeOutputDir = System.getProperty("sqoop.src.dir", ".");

    String myTmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!myTmpDir.endsWith(File.separator)) {
      myTmpDir = myTmpDir + File.separator;
    }

    this.tmpDir = myTmpDir;
    String localUsername = System.getProperty("user.name", "unknown");
    this.jarOutputDir = getNonceJarDir(tmpDir + "sqoop-" + localUsername
        + "/compile");
    this.jarDirIsAuto = true;
    this.layout = FileLayout.TextFile;

    this.areOutputDelimsManuallySet = false;
    this.areInputDelimsManuallySet = false;

    this.numMappers = DEFAULT_NUM_MAPPERS;
    this.useCompression = false;
    this.compressionCodec = null;
    this.directSplitSize = 0;

    this.maxInlineLobSize = LargeObjectLoader.DEFAULT_MAX_LOB_LENGTH;

    // Don't set a default value for fetchsize. This allows a JDBCManager to
    // provide a database-specific default, if no value is provided by the
    // user.
    this.fetchSize = null;

    if (null == baseConfiguration) {
      this.conf = new Configuration();
    } else {
      this.conf = baseConfiguration;
    }

    this.extraArgs = null;

    this.dbOutColumns = null;

    this.incrementalMode = IncrementalMode.None;

    this.updateMode = UpdateMode.UpdateOnly;

    // Creating instances for user specific mapping
    this.mapColumnHive = new Properties();
    this.mapColumnJava = new Properties();

    // Set Accumulo batch size defaults, since 0 is not the same
    // as "not set"
    this.accumuloBatchSize = AccumuloConstants.DEFAULT_BATCH_SIZE;
    this.accumuloMaxLatency = AccumuloConstants.DEFAULT_LATENCY;

    // We do not want to be verbose too much if not explicitly needed
    this.verbose = false;
    //This name of the system property is intentionally OLD_SQOOP_TEST_IMPORT_ROOT_DIR
    //to support backward compatibility. Do not exchange it with
    //org.apache.sqoop.tool.BaseSqoopTool#TEMP_ROOTDIR_ARG
    this.tempRootDir = System.getProperty(OLD_SQOOP_TEST_IMPORT_ROOT_DIR, "_sqoop");

    //This default value is set intentionally according to SQOOP_RETHROW_PROPERTY system property
    //to support backward compatibility. Do not exchange it.
    this.throwOnError = isSqoopRethrowSystemPropertySet();

    setOracleEscapingDisabled(Boolean.parseBoolean(System.getProperty(ORACLE_ESCAPING_DISABLED, "true")));

    this.isValidationEnabled = false; // validation is disabled by default
    this.validatorClass = RowCountValidator.class;
    this.validationThresholdClass = AbsoluteValidationThreshold.class;
    this.validationFailureHandlerClass = AbortOnFailureHandler.class;

    // Relaxed isolation will not enabled by default which is the behavior
    // of sqoop until now.
    this.relaxedIsolation = false;

    // set default mainframe data set type to partitioned data set
    this.mainframeInputDatasetType = MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_PARTITIONED;

    // set default metadata transaction isolation level to TRANSACTION_READ_COMMITTED
    this.metadataTransactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    // set escape column mapping to true
    this.escapeColumnMappingEnabled = true;
  }

  /**
   * The SQOOP_RETHROW_PROPERTY system property is considered to be set if it is set to
   * any kind of String value, i.e. it is not null.
   */
  // Type of SQOOP_RETHROW_PROPERTY is String only to provide backward compatibility.
  public static boolean isSqoopRethrowSystemPropertySet() {
    return (System.getProperty(SQOOP_RETHROW_PROPERTY) != null);
  }

  /**
   * Given a string containing a single character or an escape sequence
   * representing a char, return that char itself.
   *
   * Normal literal characters return themselves: "x" -&gt; 'x', etc.
   * Strings containing a '\' followed by one of t, r, n, or b escape to the
   * usual character as seen in Java: "\n" -&gt; (newline), etc.
   *
   * Strings like "\0ooo" return the character specified by the octal sequence
   * 'ooo'. Strings like "\0xhhh" or "\0Xhhh" return the character specified by
   * the hex sequence 'hhh'.
   *
   * If the input string contains leading or trailing spaces, these are
   * ignored.
   */
  public static char toChar(String charish) throws InvalidOptionsException {
    if (null == charish || charish.length() == 0) {
      throw new InvalidOptionsException("Character argument expected."
          + "\nTry --help for usage instructions.");
    }

    if (charish.startsWith("\\0x") || charish.startsWith("\\0X")) {
      if (charish.length() == 3) {
        throw new InvalidOptionsException(
            "Base-16 value expected for character argument."
            + "\nTry --help for usage instructions.");
      } else {
        String valStr = charish.substring(3);
        int val = Integer.parseInt(valStr, 16);
        return (char) val;
      }
    } else if (charish.startsWith("\\0")) {
      if (charish.equals("\\0")) {
        // it's just '\0', which we can take as shorthand for nul.
        return DelimiterSet.NULL_CHAR;
      } else {
        // it's an octal value.
        String valStr = charish.substring(2);
        int val = Integer.parseInt(valStr, 8);
        return (char) val;
      }
    } else if (charish.startsWith("\\")) {
      if (charish.length() == 1) {
        // it's just a '\'. Keep it literal.
        return '\\';
      } else if (charish.length() > 2) {
        // we don't have any 3+ char escape strings.
        throw new InvalidOptionsException(
            "Cannot understand character argument: " + charish
            + "\nTry --help for usage instructions.");
      } else {
        // this is some sort of normal 1-character escape sequence.
        char escapeWhat = charish.charAt(1);
        switch(escapeWhat) {
        case 'b':
          return '\b';
        case 'n':
          return '\n';
        case 'r':
          return '\r';
        case 't':
          return '\t';
        case '\"':
          return '\"';
        case '\'':
          return '\'';
        case '\\':
          return '\\';
        default:
          throw new InvalidOptionsException(
              "Cannot understand character argument: " + charish
              + "\nTry --help for usage instructions.");
        }
      }
    } else {
      // it's a normal character.
      if (charish.length() > 1) {
        LOG.warn("Character argument " + charish + " has multiple characters; "
            + "only the first will be used.");
      }

      return charish.charAt(0);
    }
  }

  public boolean getVerbose() {
    return verbose;
  }

  public void setVerbose(boolean beVerbose) {
    this.verbose = beVerbose;
  }

  public String getTempRootDir() {
    return tempRootDir;
  }

  public void setTempRootDir(String tempRootDir) {
    this.tempRootDir = tempRootDir;
  }

  public boolean isThrowOnError() {
    return throwOnError;
  }

  public void setThrowOnError(boolean throwOnError) {
    this.throwOnError = throwOnError;
  }

  /**
   * Get the temporary directory; guaranteed to end in File.separator
   * (e.g., '/').
   */
  public String getTmpDir() {
    return tmpDir;
  }

  public void setTmpDir(String tmp) {
    this.tmpDir = tmp;
  }

  public String getConnectString() {
    return connectString;
  }

  public void setConnectString(String connectStr) {
    this.connectString = connectStr;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String table) {
    this.tableName = table;
  }

  public String getStagingTableName() {
    return stagingTableName;
  }

  public void setStagingTableName(String stagingTable) {
    this.stagingTableName = stagingTable;
  }

  public boolean doClearStagingTable() {
    return clearStagingTable;
  }

  public void setClearStagingTable(boolean clear) {
    clearStagingTable = clear;
  }

  public String getExportDir() {
    return exportDir;
  }

  public void setExportDir(String dir) {
    this.exportDir = dir;
  }

  public String getExistingJarName() {
    return existingJarFile;
  }

  public void setExistingJarName(String jarFile) {
    this.existingJarFile = jarFile;
  }

  public String[] getColumns() {
    if (null == columns) {
      return null;
    } else {
      return Arrays.copyOf(columns, columns.length);
    }
  }

  public String getColumnNameCaseInsensitive(String col){
    if (null != columns) {
      for(String columnName : columns) {
        if(columnName.equalsIgnoreCase(col)) {
          return columnName;
        }
      }
    }
    return null;
  }

  public void setColumns(String [] cols) {
    if (null == cols) {
      this.columns = null;
    } else {
      this.columns = Arrays.copyOf(cols, cols.length);
    }
  }

  public String getSplitByCol() {
    return splitByCol;
  }

  public void setSplitByCol(String splitBy) {
    this.splitByCol = splitBy;
  }

  public Integer getSplitLimit() {
    return splitLimit;
  }

  public void setSplitLimit(Integer splitLimit) {
    this.splitLimit = splitLimit;
  }

  public String getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(String where) {
    this.whereClause = where;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String user) {
    this.username = user;
  }

  public String getPassword() {
    return password;
  }

  public String getPasswordFilePath() {
    return passwordFilePath;
  }

  public void setPasswordFilePath(String passwdFilePath) {
    this.passwordFilePath = passwdFilePath;
  }

  public String getPasswordAlias() {
    return passwordAlias;
  }

  public void setPasswordAlias(String alias) {
    this.passwordAlias = alias;
  }

  protected void parseColumnMapping(String mapping,
          Properties output) {
    output.clear();

    // replace (xx,xx) with (xx#xx), so that we can just split by "," afterwards
    String[] maps = mapping.replaceAll("\\(([0-9]+),([0-9]+)\\)", "($1#$2)").split(",");

    for(String map : maps) {
      String[] details = map.split("=");
      if (details.length != 2) {
        throw new IllegalArgumentException("Malformed mapping.  "
            + "Column mapping should be the form key=value[,key=value]*");
      }

      try {
        output.put(
            URLDecoder.decode(details[0].replaceAll("\\(([0-9]+)#([0-9]+)\\)", "($1,$2)"), "UTF-8"),
            URLDecoder.decode(details[1].replaceAll("\\(([0-9]+)#([0-9]+)\\)", "($1,$2)"), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException("Encoding not supported. "
            + "Column mapping should be UTF-8 encoding.");
      }
    }
  }

  public void setMapColumnHive(String mapColumn) {
    parseColumnMapping(mapColumn, mapColumnHive);
  }

  public void setMapColumnJava(String mapColumn) {
    parseColumnMapping(mapColumn, mapColumnJava);
  }

  public Properties getMapColumnHive() {
    return mapColumnHive;
  }

  public Properties getMapColumnJava() {
    return mapColumnJava;
  }

  /**
   * Allow the user to enter his password on the console without printing
   * characters.
   * @return the password as a string
   */
  private String securePasswordEntry() {
    try {
      return new String(System.console().readPassword("Enter password: "));
    } catch (NullPointerException e) {
      LOG.error("It seems that you have launched a Sqoop metastore job via");
      LOG.error("Oozie with sqoop.metastore.client.record.password disabled.");
      LOG.error("But this configuration is not supported because Sqoop can't");
      LOG.error("prompt the user to enter the password while being executed");
      LOG.error("as Oozie tasks. Please enable sqoop.metastore.client.record");
      LOG.error(".password in sqoop-site.xml, or provide the password");
      LOG.error("explicitly using --password in the command tag of the Oozie");
      LOG.error("workflow file.");
      return null;
    }
  }

  /**
   * Set the password in this SqoopOptions from the console without printing
   * characters.
   */
  public void setPasswordFromConsole() {
    this.password = securePasswordEntry();
  }

  public String getMapreduceJobName() {
    return mapreduceJobName;
  }

  public void setMapreduceJobName(String mapredJobName) {
    this.mapreduceJobName = mapredJobName;
  }

  public void setPassword(String pass) {
    this.password = pass;
  }

  public boolean isDirect() {
    return direct;
  }

  public void setDirectMode(boolean isDirect) {
    this.direct = isDirect;
  }

  /**
   * @return true if underlying statements to be executed in batch mode,
   * or false if to be executed in a single multirow statement.
   */
  public boolean isBatchMode() {
    return batchMode;
  }

  public void setBatchMode(boolean mode) {
    this.batchMode = mode;
  }

  /**
   * @return the number of map tasks to use for import.
   */
  public int getNumMappers() {
    return this.numMappers;
  }

  public void setNumMappers(int m) {
    this.numMappers = m;
  }

  /**
   * @return the user-specified absolute class name for the table.
   */
  public String getClassName() {
    return className;
  }

  public void setClassName(String name) {
    this.className = name;
  }

  /**
   * @return the user-specified package to prepend to table names via
   * --package-name.
   */
  public String getPackageName() {
    return packageName;
  }

  public void setPackageName(String name) {
    this.packageName = name;
  }

  public String getHiveHome() {
    return hiveHome;
  }

  public void setHiveHome(String home) {
    this.hiveHome = home;
  }

  /** @return true if we should import the table into Hive. */
  public boolean doHiveImport() {
    return hiveImport;
  }

  public void setHiveImport(boolean doImport) {
    this.hiveImport = doImport;
  }

  public String getHiveExternalTableDir() {
    return this.hiveExternalTableDir;
  }

  public void setHiveExternalTableDir(String location) {
    this.hiveExternalTableDir = location;
  }

  /**
   * @return the user-specified option to overwrite existing table in hive.
   */
  public boolean doOverwriteHiveTable() {
    return overwriteHiveTable;
  }

  public void setOverwriteHiveTable(boolean overwrite) {
    this.overwriteHiveTable = overwrite;
  }

  /**
   * @return the user-specified option to modify fields to drop hive delimiters
   */
  public boolean doHiveDropDelims() {
    return hiveDropDelims;
  }

  public void setHiveDropDelims(boolean dropHiveDelims) {
    this.hiveDropDelims = dropHiveDelims;
  }

  /**
   * @return the user-specified option to specify the replacement string
   *         for hive delimeters
   */
  public String getHiveDelimsReplacement() {
    return hiveDelimsReplacement;
  }

  public void setHiveDelimsReplacement(String replacement) {
    this.hiveDelimsReplacement = replacement;
  }

  /**
   * @return the user-specified option to specify sqoop's behavior during
   *         target table creation if the table exists.
   */
  public boolean doFailIfHiveTableExists() {
    return failIfHiveTableExists;
  }

  public void setFailIfHiveTableExists(boolean fail) {
    this.failIfHiveTableExists = fail;
  }

  // HCatalog support
  public void setHCatTableName(String ht) {
    this.hCatTableName = ht;
  }

  public String getHCatTableName() {
    return this.hCatTableName;
  }

  public void setHCatDatabaseName(String hd) {
    this.hCatDatabaseName = hd;
  }

  public String getHCatDatabaseName() {
    return this.hCatDatabaseName;
  }


  public String getHCatHome() {
    return hCatHome;
  }

  public void setHCatHome(String home) {
    this.hCatHome = home;
  }

  public boolean doCreateHCatalogTable() {
    return hCatCreateTable;
  }

  public void setCreateHCatalogTable(boolean create) {
    this.hCatCreateTable = create;
  }

  public boolean doDropAndCreateHCatalogTable() {
    return hCatDropAndCreateTable;
  }

  public void setDropAndCreateHCatalogTable(boolean dropAndCreate) {
    this.hCatDropAndCreateTable = dropAndCreate;
  }

  public void setHCatStorageStanza(String stanza) {
    this.hCatStorageStanza = stanza;
  }

  public String getHCatStorageStanza() {
    return this.hCatStorageStanza;
  }
  /**
   * @return location where .java files go; guaranteed to end with '/'.
   */
  public String getCodeOutputDir() {
    if (codeOutputDir.endsWith(File.separator)) {
      return codeOutputDir;
    } else {
      return codeOutputDir + File.separator;
    }
  }

  public void setCodeOutputDir(String outputDir) {
    this.codeOutputDir = outputDir;
  }

  /**
   * @return location where .jar and .class files go; guaranteed to end with
   * '/'.
   */
  public String getJarOutputDir() {
    if (jarOutputDir.endsWith(File.separator)) {
      return jarOutputDir;
    } else {
      return jarOutputDir + File.separator;
    }
  }

  public void setJarOutputDir(String outDir) {
    this.jarOutputDir = outDir;
    this.jarDirIsAuto = false;
  }

  /**
   * Return the value of $HADOOP_MAPRED_HOME.
   * @return $HADOOP_MAPRED_HOME, or null if it's not set.
   */
  public String getHadoopMapRedHome() {
    return hadoopMapRedHome;
  }

  public void setHadoopMapRedHome(String home) {
    this.hadoopMapRedHome = home;
  }

  /**
   * @return a sql command to execute and exit with.
   */
  public String getSqlQuery() {
    return sqlQuery;
  }

  public void setSqlQuery(String sqlStatement) {
    this.sqlQuery = sqlStatement;
  }

  public String getBoundaryQuery() {
    return boundaryQuery;
  }

  public void setBoundaryQuery(String sqlStatement) {
    boundaryQuery = sqlStatement;
  }

  /**
   * @return The JDBC driver class name specified with --driver.
   */
  public String getDriverClassName() {
    return driverClassName;
  }

  public void setDriverClassName(String driverClass) {
    this.driverClassName = driverClass;
  }

  /**
   * @return the base destination path for table uploads.
   */
  public String getWarehouseDir() {
    return warehouseDir;
  }

  public void setWarehouseDir(String warehouse) {
    this.warehouseDir = warehouse;
  }

  public String getTargetDir() {
    return this.targetDir;
  }

  public void setTargetDir(String dir) {
    this.targetDir = dir;
  }

  public void setAppendMode(boolean doAppend) {
    this.append = doAppend;
  }

  public boolean isAppendMode() {
    return this.append;
  }

  public void setDeleteMode(boolean doDelete) {
    this.delete = doDelete;
  }

  public boolean isDeleteMode() {
    return this.delete;
  }

  /**
   * @return the destination file format
   */
  public FileLayout getFileLayout() {
    return this.layout;
  }

  public void setFileLayout(FileLayout fileLayout) {
    this.layout = fileLayout;
  }

  /**
   * @return the field delimiter to use when parsing lines. Defaults to the
   * field delim to use when printing lines.
   */
  public char getInputFieldDelim() {
    char f = inputDelimiters.getFieldsTerminatedBy();
    if (f == DelimiterSet.NULL_CHAR) {
      return this.outputDelimiters.getFieldsTerminatedBy();
    } else {
      return f;
    }
  }

  /**
   * Set the field delimiter to use when parsing lines.
   */
  public void setInputFieldsTerminatedBy(char c) {
    this.inputDelimiters.setFieldsTerminatedBy(c);
  }

  /**
   * @return the record delimiter to use when parsing lines. Defaults to the
   * record delim to use when printing lines.
   */
  public char getInputRecordDelim() {
    char r = inputDelimiters.getLinesTerminatedBy();
    if (r == DelimiterSet.NULL_CHAR) {
      return this.outputDelimiters.getLinesTerminatedBy();
    } else {
      return r;
    }
  }

  /**
   * Set the record delimiter to use when parsing lines.
   */
  public void setInputLinesTerminatedBy(char c) {
    this.inputDelimiters.setLinesTerminatedBy(c);
  }

  /**
   * @return the character that may enclose fields when parsing lines.
   * Defaults to the enclosing-char to use when printing lines.
   */
  public char getInputEnclosedBy() {
    char c = inputDelimiters.getEnclosedBy();
    if (c == DelimiterSet.NULL_CHAR) {
      return this.outputDelimiters.getEnclosedBy();
    } else {
      return c;
    }
  }

  /**
   * Set the enclosed-by character to use when parsing lines.
   */
  public void setInputEnclosedBy(char c) {
    this.inputDelimiters.setEnclosedBy(c);
  }

  /**
   * @return the escape character to use when parsing lines. Defaults to the
   * escape character used when printing lines.
   */
  public char getInputEscapedBy() {
    char c = inputDelimiters.getEscapedBy();
    if (c == DelimiterSet.NULL_CHAR) {
      return this.outputDelimiters.getEscapedBy();
    } else {
      return c;
    }
  }

  /**
   * Set the escaped-by character to use when parsing lines.
   */
  public void setInputEscapedBy(char c) {
    this.inputDelimiters.setEscapedBy(c);
  }

  /**
   * @return true if fields must be enclosed by the --enclosed-by character
   * when parsing.  Defaults to false. Set true when --input-enclosed-by is
   * used.
   */
  public boolean isInputEncloseRequired() {
    char c = this.inputDelimiters.getEnclosedBy();
    if (c == DelimiterSet.NULL_CHAR) {
      return this.outputDelimiters.isEncloseRequired();
    } else {
      return this.inputDelimiters.isEncloseRequired();
    }
  }

  /**
   * If true, then all input fields are expected to be enclosed by the
   * enclosed-by character when parsing.
   */
  public void setInputEncloseRequired(boolean required) {
    this.inputDelimiters.setEncloseRequired(required);
  }

  /**
   * @return the character to print between fields when importing them to
   * text.
   */
  public char getOutputFieldDelim() {
    return this.outputDelimiters.getFieldsTerminatedBy();
  }

  /**
   * Set the field delimiter to use when formatting lines.
   */
  public void setFieldsTerminatedBy(char c) {
    this.outputDelimiters.setFieldsTerminatedBy(c);
  }


  /**
   * @return the character to print between records when importing them to
   * text.
   */
  public char getOutputRecordDelim() {
    return this.outputDelimiters.getLinesTerminatedBy();
  }

  /**
   * Set the record delimiter to use when formatting lines.
   */
  public void setLinesTerminatedBy(char c) {
    this.outputDelimiters.setLinesTerminatedBy(c);
  }

  /**
   * @return a character which may enclose the contents of fields when
   * imported to text.
   */
  public char getOutputEnclosedBy() {
    return this.outputDelimiters.getEnclosedBy();
  }

  /**
   * Set the enclosed-by character to use when formatting lines.
   */
  public void setEnclosedBy(char c) {
    this.outputDelimiters.setEnclosedBy(c);
  }

  /**
   * @return a character which signifies an escape sequence when importing to
   * text.
   */
  public char getOutputEscapedBy() {
    return this.outputDelimiters.getEscapedBy();
  }

  /**
   * Set the escaped-by character to use when formatting lines.
   */
  public void setEscapedBy(char c) {
    this.outputDelimiters.setEscapedBy(c);
  }

  /**
   * @return true if fields imported to text must be enclosed by the
   * EnclosedBy char.  default is false; set to true if --enclosed-by is used
   * instead of --optionally-enclosed-by.
   */
  public boolean isOutputEncloseRequired() {
    return this.outputDelimiters.isEncloseRequired();
  }

  /**
   * If true, then the enclosed-by character will be applied to all fields,
   * even if internal characters do not need enclosed-by protection.
   */
  public void setOutputEncloseRequired(boolean required) {
    this.outputDelimiters.setEncloseRequired(required);
  }

  /**
   * @return the set of delimiters used for formatting output records.
   */
  public DelimiterSet getOutputDelimiters() {
    return this.outputDelimiters.copy();
  }

  /**
   * Set the complete set of delimiters to use for output formatting.
   */
  public void setOutputDelimiters(DelimiterSet delimiters) {
    this.outputDelimiters = delimiters.copy();
  }

  /**
   * @return the set of delimiters used for parsing the input.
   * This may include values implicitly set by the output delimiters.
   */
  public DelimiterSet getInputDelimiters() {
    return new DelimiterSet(
        getInputFieldDelim(),
        getInputRecordDelim(),
        getInputEnclosedBy(),
        getInputEscapedBy(),
        isInputEncloseRequired());
  }

  /**
   * @return true if the user wants imported results to be compressed.
   */
  public boolean shouldUseCompression() {
    return this.useCompression || compressionCodec != null;
  }

  public void setUseCompression(boolean compress) {
    this.useCompression = compress;
  }

  /**
   * @return the name of the compression codec to use when importing.
   * E.g. <code>org.apache.hadoop.io.compress.GzipCodec</code>.
   */
  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(String codec) {
    this.compressionCodec = codec;
  }
  /**
   * @return the name of the destination table when importing to Hive.
   */
  public String getHiveTableName() {
    if (null != this.hiveTableName) {
      return this.hiveTableName;
    } else {
      return this.tableName;
    }
  }

  public void setHiveTableName(String name) {
    this.hiveTableName = name;
  }

  public String getHiveDatabaseName() {
    return this.hiveDatabaseName;
  }

  public void setHiveDatabaseName(String name) {
    this.hiveDatabaseName = name;
  }

  public String getHivePartitionKey() {
    return hivePartitionKey;
  }

  public void setHivePartitionKey(String hpk) {
    this.hivePartitionKey = hpk;
  }

  public String getHivePartitionValue() {
    return hivePartitionValue;
  }

  public void setHivePartitionValue(String hpv) {
    this.hivePartitionValue = hpv;
  }

  /**
   * @return the file size to split by when using --direct mode.
   */
  public long getDirectSplitSize() {
    return this.directSplitSize;
  }

  public void setDirectSplitSize(long splitSize) {
    this.directSplitSize = splitSize;
  }

  /**
   * @return the max size of a LOB before we spill to a separate file.
   */
  public long getInlineLobLimit() {
    return this.maxInlineLobSize;
  }

  public void setInlineLobLimit(long limit) {
    this.maxInlineLobSize = limit;
  }

  public Integer getFetchSize() {
    return this.fetchSize;
  }

  public void setFetchSize(Integer size) {
    this.fetchSize = size;
  }

  /*
   * @return true if the output delimiters have been explicitly set by the user
   */
  public boolean explicitOutputDelims() {
    return areOutputDelimsManuallySet;
  }

  /**
   * Flag the output delimiter settings as explicit user settings, or implicit.
   */
  public void setExplicitOutputDelims(boolean explicit) {
    this.areOutputDelimsManuallySet = explicit;
  }

  /**
   * @return true if the input delimiters have been explicitly set by the user.
   */
  public boolean explicitInputDelims() {
    return areInputDelimsManuallySet;
  }

  /**
   * Flag the input delimiter settings as explicit user settings, or implicit.
    */
  public void setExplicitInputDelims(boolean explicit) {
    this.areInputDelimsManuallySet = explicit;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration config) {
    this.conf = config;
  }

  /**
   * @return command-line arguments after a '-'.
   */
  public String [] getExtraArgs() {
    if (extraArgs == null) {
      return null;
    }

    String [] out = new String[extraArgs.length];
    for (int i = 0; i < extraArgs.length; i++) {
      out[i] = extraArgs[i];
    }
    return out;
  }

  public void setExtraArgs(String [] args) {
    if (null == args) {
      this.extraArgs = null;
      return;
    }

    this.extraArgs = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      this.extraArgs[i] = args[i];
    }
  }

  /**
   * Set the name of the column to be used in the WHERE clause of an
   * UPDATE-based export process.
   */
  public void setUpdateKeyCol(String colName) {
    this.updateKeyCol = colName;
  }

  /**
   * @return the column which is the key column in a table to be exported
   * in update mode.
   */
  public String getUpdateKeyCol() {
    return this.updateKeyCol;
  }

  /**
   * Set "UpdateOnly" to silently ignore new rows during update export.
   * Set "AllowInsert" to insert new rows during update export.
   */
  public void setUpdateMode(UpdateMode mode) {
    this.updateMode = mode;
  }

  /**
   * @return how to handle new rows found in update export.
   */
  public UpdateMode getUpdateMode() {
    return updateMode;
  }

  /**
   * @return an ordered list of column names. The code generator should
   * generate the DBWritable.write(PreparedStatement) method with columns
   * exporting in this order, if it is non-null.
   */
  public String [] getDbOutputColumns() {
    if (null != dbOutColumns) {
      return Arrays.copyOf(this.dbOutColumns, dbOutColumns.length);
    } else {
      return null;
    }
  }

  /**
   * Set the order in which columns should be serialized by the generated
   * DBWritable.write(PreparedStatement) method. Setting this to null will use
   * the "natural order" of the database table.
   *
   * TODO: Expose this setter via the command-line arguments for the codegen
   * module. That would allow users to export to tables with columns in a
   * different physical order than the file layout in HDFS.
   */
  public void setDbOutputColumns(String [] outCols) {
    if (null == outCols) {
      this.dbOutColumns = null;
    } else {
      this.dbOutColumns = Arrays.copyOf(outCols, outCols.length);
    }
  }

  /**
   * Set whether we should create missing HBase tables.
   */
  public void setCreateHBaseTable(boolean create) {
    this.hbaseCreateTable = create;
  }

  /**
   * Returns true if we should create HBase tables/column families
   * that are missing.
   */
  public boolean getCreateHBaseTable() {
    return this.hbaseCreateTable;
  }

  /**
   * Sets the HBase target column family.
   */
  public void setHBaseColFamily(String colFamily) {
    this.hbaseColFamily = colFamily;
  }

  /**
   * Gets the HBase import target column family.
   */
  public String getHBaseColFamily() {
    return this.hbaseColFamily;
  }

  /**
   * Gets the column to use as the row id in an hbase import.
   * If null, use the primary key column.
   */
  public String getHBaseRowKeyColumn() {
    return this.hbaseRowKeyCol;
  }

  /**
   * Sets the column to use as the row id in an hbase import.
   */
  public void setHBaseRowKeyColumn(String col) {
    this.hbaseRowKeyCol = col;
  }

  /**
   * @return true if bulk load is enabled and false otherwise.
   */
  public boolean isBulkLoadEnabled() {
    return this.hbaseBulkLoadEnabled;
  }

  /**
   * Sets the temp dir to use as the bulk load dir in an hbase import.
   */
  public void setHBaseBulkLoadEnabled(boolean hbaseBulkLoadEnabled) {
    this.hbaseBulkLoadEnabled = hbaseBulkLoadEnabled;
  }

  /**
   * Gets the target HBase table name, if any.
   */
  public String getHBaseTable() {
    return this.hbaseTable;
  }

  /**
   * Sets the target HBase table name for an import.
   */
  public void setHBaseTable(String table) {
    this.hbaseTable = table;
  }

  /**
   * Set the column of the import source table to check for incremental import
   * state.
   */
  public void setIncrementalTestColumn(String colName) {
    this.incrementalTestCol = colName;
  }

  /**
   * Return the name of the column of the import source table
   * to check for incremental import state.
   */
  public String getIncrementalTestColumn() {
    return this.incrementalTestCol;
  }

  /**
   * Set the incremental import mode to use.
   */
  public void setIncrementalMode(IncrementalMode mode) {
    this.incrementalMode = mode;
  }

  /**
   * Get the incremental import mode to use.
   */
  public IncrementalMode getIncrementalMode() {
    return this.incrementalMode;
  }

  /**
   * Set the last imported value of the incremental import test column.
   */
  public void setIncrementalLastValue(String lastVal) {
    this.incrementalLastValue = lastVal;
  }

  /**
   * Get the last imported value of the incremental import test column.
   */
  public String getIncrementalLastValue() {
    return this.incrementalLastValue;
  }

  /**
   * Set the tables to be excluded when doing all table import.
   */
  public void setAllTablesExclude(String exclude) {
    this.allTablesExclude = exclude;
  }

  /**
   * Get the tables to be excluded when doing all table import.
   */
  public String getAllTablesExclude() {
    return this.allTablesExclude;
  }

  /**
   * Set the name of the saved job this SqoopOptions belongs to.
   */
  public void setJobName(String job) {
    this.jobName = job;
  }

  /**
   * Get the name of the saved job this SqoopOptions belongs to.
   */
  public String getJobName() {
    return this.jobName;
  }

  /**
   * Set the JobStorage descriptor used to open the saved job
   * this SqoopOptions belongs to.
   */
  public void setStorageDescriptor(Map<String, String> descriptor) {
    this.jobStorageDescriptor = descriptor;
  }

  /**
   * Get the JobStorage descriptor used to open the saved job
   * this SqoopOptions belongs to.
   */
  public Map<String, String> getStorageDescriptor() {
    return this.jobStorageDescriptor;
  }

  /**
   * Return the parent instance this SqoopOptions is derived from.
   */
  public com.cloudera.sqoop.SqoopOptions getParent() {
    return this.parent;
  }

  /**
   * Set the parent instance this SqoopOptions is derived from.
   */
  public void setParent(com.cloudera.sqoop.SqoopOptions options) {
    this.parent = options;
  }

  /**
   * Set the path name used to do an incremental import of old data
   * which will be combined with an "new" dataset.
   */
  public void setMergeOldPath(String path) {
    this.mergeOldPath = path;
  }

  /**
   * Return the path name used to do an incremental import of old data
   * which will be combined with an "new" dataset.
   */
  public String getMergeOldPath() {
    return this.mergeOldPath;
  }

  /**
   * Set the path name used to do an incremental import of new data
   * which will be combined with an "old" dataset.
   */
  public void setMergeNewPath(String path) {
    this.mergeNewPath = path;
  }

  /**
   * Return the path name used to do an incremental import of new data
   * which will be combined with an "old" dataset.
   */
  public String getMergeNewPath() {
    return this.mergeNewPath;
  }

  /**
   * Set the name of the column used to merge an old and new dataset.
   */
  public void setMergeKeyCol(String col) {
    this.mergeKeyCol = col;
  }

  /** Return the name of the column used to merge an old and new dataset. */
  public String getMergeKeyCol() {
    return this.mergeKeyCol;
  }

  /**
   * Set the mainframe dataset name.
   */
  public void setMainframeInputDatasetName(String name) {
    mainframeInputDatasetName = name;
    tableName = name;
    // may need to set this in the conf variable otherwise it gets lost.
  }

  public void setMainframeInputDatasetType(String name) {
    mainframeInputDatasetType = name;
  }

  /**
   * Return the mainframe dataset name.
   */
  public String getMainframeInputDatasetName() {
    return mainframeInputDatasetName;
  }

  /*
   * Return the mainframe dataset type
   */
  public String getMainframeInputDatasetType() {
    return mainframeInputDatasetType;
  }

  // return whether the dataset is on tape
  public Boolean getMainframeInputDatasetTape() {
	  if (mainframeInputDatasetTape == null) { return false; }
	  return Boolean.parseBoolean(mainframeInputDatasetTape);
  }

  // sets whether the dataset is on tape
  public void setMainframeInputDatasetTape(String txtIsFromTape) {
	  mainframeInputDatasetTape = Boolean.valueOf(Boolean.parseBoolean(txtIsFromTape)).toString();
  }

  public static String getAccumuloHomeDefault() {
    // Set this with $ACCUMULO_HOME, but -Daccumulo.home can override.
    String accumuloHome = System.getenv("ACCUMULO_HOME");
    accumuloHome = System.getProperty("accumulo.home", accumuloHome);
    return accumuloHome;
  }

  public static String getZookeeperHomeDefault() {
    // Set this with $ZOOKEEPER_HOME, but -Dzookeeper.home can override.
    String zookeeperHome = System.getenv("ZOOKEEPER_HOME");
    zookeeperHome = System.getProperty("zookeeper.home", zookeeperHome);
    return zookeeperHome;
  }

  public String getAccumuloHome() {
    return accumuloHome;
  }

  public void setAccumuloHome(String home) {
    this.accumuloHome = home;
  }

  public String getZookeeperHome() {
    return zookeeperHome;
  }

  public void setZookeeperHome(String home) {
    this.zookeeperHome = home;
  }

  /**
   * Set whether we should create missing Accumulo tables.
   */
  public void setCreateAccumuloTable(boolean create) {
    this.accumuloCreateTable = create;
  }

  /**
   * Returns true if we should create Accumulo tables/column families
   * that are missing.
   */
  public boolean getCreateAccumuloTable() {
    return this.accumuloCreateTable;
  }

  /**
   * Sets the Accumulo batch size (in bytes).
   */
  public void setAccumuloBatchSize(long batchSize) {
    this.accumuloBatchSize = batchSize;
  }

  /**
   * Gets the Accumulo batch size (in bytes).
   */
  public long getAccumuloBatchSize() {
    return this.accumuloBatchSize;
  }

  /**
   * Sets the Accumulo target column family.
   */
  public void setAccumuloColFamily(String colFamily) {
    this.accumuloColFamily = colFamily;
  }

  /**
   * Gets the Accumulo import target column family.
   */
  public String getAccumuloColFamily() {
    return this.accumuloColFamily;
  }

  /**
   * Sets the Accumulo max latency.
   */
  public void setAccumuloMaxLatency(long maxLatency) {
    this.accumuloMaxLatency = maxLatency;
  }

  /**
   * Gets the Accumulo max latency.
   */
  public long getAccumuloMaxLatency() {
    return this.accumuloMaxLatency;
  }

  /**
   * Gets the column to use as the row id in an Accumulo import.
   * If null, use the primary key column.
   */
  public String getAccumuloRowKeyColumn() {
    return this.accumuloRowKeyCol;
  }

 /**
  * Sets the column to use as the row id in an Accumulo import.
  *
  */
  public void setAccumuloRowKeyColumn(String col) {
    this.accumuloRowKeyCol = col;
  }

  /**
   * Gets the visibility token to use.
   * If null, don't assign a visibility.
   */
  public String getAccumuloVisibility() {
    return this.accumuloVisibility;
  }

 /**
  * Sets the visibility token to use.
  *
  */
  public void setAccumuloVisibility(String vis) {
    this.accumuloVisibility = vis;
  }

  /**
   * Gets the target Accumulo table name, if any.
   */
  public String getAccumuloTable() {
    return this.accumuloTable;
  }

  /**
   * Sets the target Accumulo table name.
   */
  public void setAccumuloTable(String table) {
    this.accumuloTable = table;
  }

  /**
   * Gets the target Accumulo user name, if any.
   */
  public String getAccumuloUser() {
    return this.accumuloUser;
  }

  /**
   * Sets the target Accumulo user name for an import.
   */
  public void setAccumuloUser(String user) {
    this.accumuloUser = user;
  }

  /**
   * Gets the target Accumulo password, if any.
   */
  public String getAccumuloPassword() {
    return this.accumuloPassword;
  }

  /**
   * Sets the target Accumulo password for an import.
   */
  public void setAccumuloPassword(String passwd) {
    this.accumuloPassword = passwd;
  }

  /**
   * Gets the target Accumulo instance, if any.
   */
  public String getAccumuloInstance() {
    return this.accumuloInstance;
  }

  /**
   * Sets the target Accumulo instance for an import.
   */
  public void setAccumuloInstance(String instance) {
    this.accumuloInstance = instance;
  }

  /**
   * Gets the target Accumulo zookeeper instance, if any.
   */
  public String getAccumuloZookeepers() {
    return this.accumuloZookeepers;
  }

  /**
   ** Sets the target Accumulo zookeeper instance for an import.
   **/
  public void setAccumuloZookeepers(String zookeepers) {
    this.accumuloZookeepers = zookeepers;
  }

  public void setConnManagerClassName(String connManagerClass) {
    this.connManagerClassName = connManagerClass;
  }

  public String getConnManagerClassName() {
    return connManagerClassName;
  }

  /** @return the SqoopTool that is operating this session. */
  public SqoopTool getActiveSqoopTool() {
    return activeSqoopTool;
  }

  public void setActiveSqoopTool(SqoopTool tool) {
    activeSqoopTool = tool;
  }

  public void setNullStringValue(String nullString) {
    this.nullStringValue = nullString;
  }

  public String getNullStringValue() {
    return nullStringValue;
  }

  public void setInNullStringValue(String inNullString) {
    this.inNullStringValue = inNullString;
  }

  public String getInNullStringValue() {
    return inNullStringValue;
  }

  public void setNullNonStringValue(String nullNonString) {
    this.nullNonStringValue = nullNonString;
  }

  public String getNullNonStringValue() {
    return nullNonStringValue;
  }

  public void setInNullNonStringValue(String inNullNonString) {
    this.inNullNonStringValue = inNullNonString;
  }

  public String getInNullNonStringValue() {
    return inNullNonStringValue;
  }

  public void setConnectionParams(Properties params) {
    connectionParams = new Properties();
    connectionParams.putAll(params);
  }

  public Properties getConnectionParams() {
    return connectionParams;
  }

  public void setValidationEnabled(boolean validationEnabled) {
    isValidationEnabled = validationEnabled;
  }

  public boolean isValidationEnabled() {
    return isValidationEnabled;
  }

  public Class getValidatorClass() {
    return validatorClass;
  }

  public void setValidatorClass(Class validatorClazz) {
    this.validatorClass = validatorClazz;
  }

  public Class getValidationThresholdClass() {
    return validationThresholdClass;
  }

  public void setValidationThresholdClass(Class validationThresholdClazz) {
    this.validationThresholdClass = validationThresholdClazz;
  }

  public Class getValidationFailureHandlerClass() {
    return validationFailureHandlerClass;
  }

  public void setValidationFailureHandlerClass(
      Class validationFailureHandlerClazz) {
    this.validationFailureHandlerClass = validationFailureHandlerClazz;
  }

  public String getCall() {
    return call;
  }

  public void setCall(String theCall) {
    this.call = theCall;
  }

  public void setSkipDistCache(boolean skip) {
    this.skipDistCache = skip;
  }

  public boolean isSkipDistCache() {
    return this.skipDistCache;
  }

  public void setRelaxedIsolation(boolean b) {
    this.relaxedIsolation = true;

  }

  public boolean getRelaxedIsolation() {
    return this.relaxedIsolation;
  }

  public void setAutoResetToOneMapper(boolean b) {
	 this.autoResetToOneMapper = b;
  }

  public boolean getAutoResetToOneMapper() {
	return this.autoResetToOneMapper;
  }

  public String getHCatalogPartitionKeys() {
    return hCatalogPartitionKeys;
  }


  public void setHCatalogPartitionKeys(String hpks) {
    this.hCatalogPartitionKeys = hpks;
  }

  public String getHCatalogPartitionValues() {
    return hCatalogPartitionValues;
  }

  public void setHCatalogPartitionValues(String hpvs) {
    this.hCatalogPartitionValues = hpvs;
  }

  public Map<String, String> getCustomToolOptions() {
    return customToolOptions;
  }

  public void setCustomToolOptions(Map<String, String> customToolOptions) {
    this.customToolOptions = customToolOptions;
  }

  public String getToolName() {
    return this.toolName;
  }

  public void setToolName(String toolName) {
    this.toolName = toolName;
  }

  public int getMetadataTransactionIsolationLevel() {
    return this.metadataTransactionIsolationLevel;
  }

  public void setMetadataTransactionIsolationLevel(int transactionIsolationLevel) {
    this.metadataTransactionIsolationLevel = transactionIsolationLevel;
  }

  public boolean isOracleEscapingDisabled() {
    return oracleEscapingDisabled;
  }

  public void setOracleEscapingDisabled(boolean escapingDisabled) {
    this.oracleEscapingDisabled = escapingDisabled;
    // important to have custom setter to ensure option is available through
    // Hadoop configuration on those places where SqoopOptions is not reachable
    getConf().setBoolean(ORACLE_ESCAPING_DISABLED, escapingDisabled);
  }

  public void setEscapeMappingColumnNamesEnabled(boolean escapingEnabled) {
    this.escapeColumnMappingEnabled = escapingEnabled;
    // important to have custom setter to ensure option is available through
    // Hadoop configuration on those places where SqoopOptions is not reachable
    getConf().setBoolean(BaseSqoopTool.ESCAPE_MAPPING_COLUMN_NAMES_ENABLED, escapingEnabled);
  }

  public boolean getEscapeMappingColumnNamesEnabled() {
    return escapeColumnMappingEnabled;
  }

  public Properties getColumnNames() {
    if (escapeColumnMappingEnabled && null == mapReplacedColumnJava) {
      return doCleanColumnMapping();
    }
    return escapeColumnMappingEnabled ? mapReplacedColumnJava : mapColumnJava;
  }

  private Properties doCleanColumnMapping() {
      mapReplacedColumnJava = new Properties();

      if (!mapColumnJava.isEmpty()) {
        for (Map.Entry<Object, Object> entry : mapColumnJava.entrySet()) {
          String candidate = toJavaIdentifier((String)entry.getKey());
          mapReplacedColumnJava.put(candidate, mapColumnJava.getProperty((String)entry.getKey()));
        }
        return mapReplacedColumnJava;
      }

      return mapColumnJava;
    }


}

