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

package org.apache.sqoop.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.apache.sqoop.util.CredentialsUtil;
import org.apache.sqoop.util.LoggingUtils;
import org.apache.sqoop.util.password.CredentialProviderHelper;

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;

/**
 * Layer on top of SqoopTool that provides some basic common code
 * that most SqoopTool implementations will use.
 *
 * Subclasses should call init() at the top of their run() method,
 * and call destroy() at the end in a finally block.
 */
public abstract class BaseSqoopTool extends com.cloudera.sqoop.tool.SqoopTool {

  public static final String METADATA_TRANSACTION_ISOLATION_LEVEL = "metadata-transaction-isolation-level";

  public static final Log LOG = LogFactory.getLog(
      BaseSqoopTool.class.getName());

  public static final String HELP_STR = "\nTry --help for usage instructions.";

  // Here are all the arguments that are used by the standard sqoop tools.
  // Their names are recorded here so that tools can share them and their
  // use consistently. The argument parser applies the leading '--' to each
  // string.
  public static final String CONNECT_STRING_ARG = "connect";
  public static final String CONN_MANAGER_CLASS_NAME =
      "connection-manager";
  public static final String CONNECT_PARAM_FILE = "connection-param-file";
  public static final String DRIVER_ARG = "driver";
  public static final String USERNAME_ARG = "username";
  public static final String PASSWORD_ARG = "password";
  public static final String PASSWORD_PROMPT_ARG = "P";
  public static final String PASSWORD_PATH_ARG = "password-file";
  public static final String PASSWORD_ALIAS_ARG = "password-alias";
  public static final String DIRECT_ARG = "direct";
  public static final String BATCH_ARG = "batch";
  public static final String TABLE_ARG = "table";
  public static final String STAGING_TABLE_ARG = "staging-table";
  public static final String CLEAR_STAGING_TABLE_ARG = "clear-staging-table";
  public static final String COLUMNS_ARG = "columns";
  public static final String SPLIT_BY_ARG = "split-by";
  public static final String SPLIT_LIMIT_ARG = "split-limit";
  public static final String WHERE_ARG = "where";
  public static final String HADOOP_HOME_ARG = "hadoop-home";
  public static final String HADOOP_MAPRED_HOME_ARG = "hadoop-mapred-home";
  public static final String HIVE_HOME_ARG = "hive-home";
  public static final String WAREHOUSE_DIR_ARG = "warehouse-dir";
  public static final String TARGET_DIR_ARG = "target-dir";
  public static final String APPEND_ARG = "append";
  public static final String DELETE_ARG = "delete-target-dir";
  public static final String NULL_STRING = "null-string";
  public static final String INPUT_NULL_STRING = "input-null-string";
  public static final String NULL_NON_STRING = "null-non-string";
  public static final String INPUT_NULL_NON_STRING = "input-null-non-string";
  public static final String MAP_COLUMN_JAVA = "map-column-java";
  public static final String MAP_COLUMN_HIVE = "map-column-hive";

  public static final String FMT_SEQUENCEFILE_ARG = "as-sequencefile";
  public static final String FMT_TEXTFILE_ARG = "as-textfile";
  public static final String FMT_AVRODATAFILE_ARG = "as-avrodatafile";
  public static final String FMT_PARQUETFILE_ARG = "as-parquetfile";
  public static final String HIVE_IMPORT_ARG = "hive-import";
  public static final String HIVE_TABLE_ARG = "hive-table";
  public static final String HIVE_DATABASE_ARG = "hive-database";
  public static final String HIVE_OVERWRITE_ARG = "hive-overwrite";
  public static final String HIVE_DROP_DELIMS_ARG = "hive-drop-import-delims";
  public static final String HIVE_DELIMS_REPLACEMENT_ARG =
          "hive-delims-replacement";
  public static final String HIVE_PARTITION_KEY_ARG = "hive-partition-key";
  public static final String HIVE_PARTITION_VALUE_ARG = "hive-partition-value";
  public static final String HIVE_EXTERNAL_TABLE_LOCATION_ARG = "external-table-dir";
  public static final String HCATCALOG_PARTITION_KEYS_ARG =
      "hcatalog-partition-keys";
  public static final String HCATALOG_PARTITION_VALUES_ARG =
      "hcatalog-partition-values";
  public static final String CREATE_HIVE_TABLE_ARG =
      "create-hive-table";
  public static final String HCATALOG_TABLE_ARG = "hcatalog-table";
  public static final String HCATALOG_DATABASE_ARG = "hcatalog-database";
  public static final String CREATE_HCATALOG_TABLE_ARG =
    "create-hcatalog-table";
  public static final String DROP_AND_CREATE_HCATALOG_TABLE =
    "drop-and-create-hcatalog-table";
  public static final String HCATALOG_STORAGE_STANZA_ARG =
    "hcatalog-storage-stanza";
  public static final String HCATALOG_HOME_ARG = "hcatalog-home";
  public static final String MAPREDUCE_JOB_NAME = "mapreduce-job-name";
  public static final String NUM_MAPPERS_ARG = "num-mappers";
  public static final String NUM_MAPPERS_SHORT_ARG = "m";
  public static final String COMPRESS_ARG = "compress";
  public static final String COMPRESSION_CODEC_ARG = "compression-codec";
  public static final String COMPRESS_SHORT_ARG = "z";
  public static final String DIRECT_SPLIT_SIZE_ARG = "direct-split-size";
  public static final String INLINE_LOB_LIMIT_ARG = "inline-lob-limit";
  public static final String FETCH_SIZE_ARG = "fetch-size";
  public static final String EXPORT_PATH_ARG = "export-dir";
  public static final String FIELDS_TERMINATED_BY_ARG = "fields-terminated-by";
  public static final String LINES_TERMINATED_BY_ARG = "lines-terminated-by";
  public static final String OPTIONALLY_ENCLOSED_BY_ARG =
      "optionally-enclosed-by";
  public static final String ENCLOSED_BY_ARG = "enclosed-by";
  public static final String ESCAPED_BY_ARG = "escaped-by";
  public static final String MYSQL_DELIMITERS_ARG = "mysql-delimiters";
  public static final String INPUT_FIELDS_TERMINATED_BY_ARG =
      "input-fields-terminated-by";
  public static final String INPUT_LINES_TERMINATED_BY_ARG =
      "input-lines-terminated-by";
  public static final String INPUT_OPTIONALLY_ENCLOSED_BY_ARG =
      "input-optionally-enclosed-by";
  public static final String INPUT_ENCLOSED_BY_ARG = "input-enclosed-by";
  public static final String INPUT_ESCAPED_BY_ARG = "input-escaped-by";
  public static final String CODE_OUT_DIR_ARG = "outdir";
  public static final String BIN_OUT_DIR_ARG = "bindir";
  public static final String PACKAGE_NAME_ARG = "package-name";
  public static final String CLASS_NAME_ARG = "class-name";
  public static final String JAR_FILE_NAME_ARG = "jar-file";
  public static final String SQL_QUERY_ARG = "query";
  public static final String SQL_QUERY_BOUNDARY = "boundary-query";
  public static final String SQL_QUERY_SHORT_ARG = "e";
  public static final String VERBOSE_ARG = "verbose";
  public static final String HELP_ARG = "help";
  public static final String TEMP_ROOTDIR_ARG = "temporary-rootdir";
  public static final String UPDATE_KEY_ARG = "update-key";
  public static final String UPDATE_MODE_ARG = "update-mode";
  public static final String CALL_ARG = "call";
  public static final String SKIP_DISTCACHE_ARG = "skip-dist-cache";
  public static final String RELAXED_ISOLATION = "relaxed-isolation";
  public static final String THROW_ON_ERROR_ARG = "throw-on-error";
  public static final String ORACLE_ESCAPING_DISABLED = "oracle-escaping-disabled";
  public static final String ESCAPE_MAPPING_COLUMN_NAMES_ENABLED = "escape-mapping-column-names";

  // Arguments for validation.
  public static final String VALIDATE_ARG = "validate";
  public static final String VALIDATOR_CLASS_ARG = "validator";
  public static final String VALIDATION_THRESHOLD_CLASS_ARG =
      "validation-threshold";
  public static final String VALIDATION_FAILURE_HANDLER_CLASS_ARG =
      "validation-failurehandler";

  // Arguments for incremental imports.
  public static final String INCREMENT_TYPE_ARG = "incremental";
  public static final String INCREMENT_COL_ARG = "check-column";
  public static final String INCREMENT_LAST_VAL_ARG = "last-value";

  // Arguments for all table imports.
  public static final String ALL_TABLE_EXCLUDES_ARG = "exclude-tables";

  // HBase arguments.
  public static final String HBASE_TABLE_ARG = "hbase-table";
  public static final String HBASE_COL_FAM_ARG = "column-family";
  public static final String HBASE_ROW_KEY_ARG = "hbase-row-key";
  public static final String HBASE_BULK_LOAD_ENABLED_ARG =
      "hbase-bulkload";
  public static final String HBASE_CREATE_TABLE_ARG = "hbase-create-table";

  //Accumulo arguments.
  public static final String ACCUMULO_TABLE_ARG = "accumulo-table";
  public static final String ACCUMULO_COL_FAM_ARG = "accumulo-column-family";
  public static final String ACCUMULO_ROW_KEY_ARG = "accumulo-row-key";
  public static final String ACCUMULO_VISIBILITY_ARG = "accumulo-visibility";
  public static final String ACCUMULO_CREATE_TABLE_ARG
      = "accumulo-create-table";
  public static final String ACCUMULO_BATCH_SIZE_ARG = "accumulo-batch-size";
  public static final String ACCUMULO_MAX_LATENCY_ARG = "accumulo-max-latency";
  public static final String ACCUMULO_ZOOKEEPERS_ARG = "accumulo-zookeepers";
  public static final String ACCUMULO_INSTANCE_ARG = "accumulo-instance";
  public static final String ACCUMULO_USER_ARG = "accumulo-user";
  public static final String ACCUMULO_PASSWORD_ARG = "accumulo-password";


  // Arguments for the saved job management system.
  public static final String STORAGE_METASTORE_ARG = "meta-connect";
  public static final String JOB_CMD_CREATE_ARG = "create";
  public static final String JOB_CMD_DELETE_ARG = "delete";
  public static final String JOB_CMD_EXEC_ARG = "exec";
  public static final String JOB_CMD_LIST_ARG = "list";
  public static final String JOB_CMD_SHOW_ARG = "show";

  // Arguments for the metastore.
  public static final String METASTORE_SHUTDOWN_ARG = "shutdown";


  // Arguments for merging datasets.
  public static final String NEW_DATASET_ARG = "new-data";
  public static final String OLD_DATASET_ARG = "onto";
  public static final String MERGE_KEY_ARG = "merge-key";

  // Reset number of mappers to one if there is no primary key avaliable and
  // split by column is explicitly not provided

  public static final String AUTORESET_TO_ONE_MAPPER = "autoreset-to-one-mapper";

  static final String HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED = "--incremental lastmodified option for hive imports is not "
      + "supported. Please remove the parameter --incremental lastmodified.";


  public BaseSqoopTool() {
  }

  public BaseSqoopTool(String toolName) {
    super(toolName);
  }

  protected ConnManager manager;

  public ConnManager getManager() {
    return manager;
  }

  public void setManager(ConnManager mgr) {
    this.manager = mgr;
  }

  /**
   * Should be called at the beginning of the run() method to initialize
   * the connection manager, etc. If this succeeds (returns true), it should
   * be paired with a call to destroy().
   * @return true on success, false on failure.
   */
  protected boolean init(SqoopOptions sqoopOpts) {
    // Get the connection to the database.
      // Set the tool name in sqoop options
      sqoopOpts.setToolName(getToolName());
    try {
      JobData data = new JobData(sqoopOpts, this);
      this.manager = new ConnFactory(sqoopOpts.getConf()).getManager(data);
      return true;
    } catch (Exception e) {
      LOG.error("Got error creating database manager: "
          + StringUtils.stringifyException(e));
      rethrowIfRequired(sqoopOpts, e);
    }

    return false;
  }

  protected void rethrowIfRequired(SqoopOptions options, Exception ex) {
    if (!options.isThrowOnError()) {
      return;
    }

    final RuntimeException exceptionToThrow;
    if (ex instanceof RuntimeException) {
      exceptionToThrow = (RuntimeException) ex;
    } else {
      exceptionToThrow = new RuntimeException(ex);
    }

    throw exceptionToThrow;
  }


  /**
   * Should be called in a 'finally' block at the end of the run() method.
   */
  protected void destroy(SqoopOptions sqoopOpts) {
    if (null != manager) {
      try {
        manager.close();
      } catch (SQLException sqlE) {
        LOG.warn("Error while closing connection: " + sqlE);
      }
    }
  }

  /**
   * Examines a subset of the arrray presented, and determines if it
   * contains any non-empty arguments. If so, logs the arguments
   * and returns true.
   *
   * @param argv an array of strings to check.
   * @param offset the first element of the array to check
   * @param len the number of elements to check
   * @return true if there are any non-null, non-empty argument strings
   * present.
   */
  protected boolean hasUnrecognizedArgs(String [] argv, int offset, int len) {
    if (argv == null) {
      return false;
    }

    boolean unrecognized = false;
    boolean printedBanner = false;
    for (int i = offset; i < Math.min(argv.length, offset + len); i++) {
      if (argv[i] != null && argv[i].length() > 0) {
        if (!printedBanner) {
          LOG.error("Error parsing arguments for " + getToolName() + ":");
          printedBanner = true;
        }
        LOG.error("Unrecognized argument: " + argv[i]);
        unrecognized = true;
      }
    }

    return unrecognized;
  }

  protected boolean hasUnrecognizedArgs(String [] argv) {
    if (null == argv) {
      return false;
    }
    return hasUnrecognizedArgs(argv, 0, argv.length);
  }


  /**
   * If argv contains an entry "--", return an array containing all elements
   * after the "--" separator. Otherwise, return null.
   * @param argv a set of arguments to scan for the subcommand arguments.
   */
  protected String [] getSubcommandArgs(String [] argv) {
    if (null == argv) {
      return null;
    }

    for (int i = 0; i < argv.length; i++) {
      if (argv[i].equals("--")) {
        return Arrays.copyOfRange(argv, i + 1, argv.length);
      }
    }

    return null;
  }

  /**
   * @return RelatedOptions used by job management tools.
   */
  protected RelatedOptions getJobOptions() {
    RelatedOptions relatedOpts = new RelatedOptions(
        "Job management arguments");
    relatedOpts.addOption(OptionBuilder.withArgName("jdbc-uri")
        .hasArg()
        .withDescription("Specify JDBC connect string for the metastore")
        .withLongOpt(STORAGE_METASTORE_ARG)
        .create());

    // Create an option-group surrounding the operations a user
    // can perform on jobs.
    OptionGroup group = new OptionGroup();
    group.addOption(OptionBuilder.withArgName("job-id")
        .hasArg()
        .withDescription("Create a new saved job")
        .withLongOpt(JOB_CMD_CREATE_ARG)
        .create());
    group.addOption(OptionBuilder.withArgName("job-id")
        .hasArg()
        .withDescription("Delete a saved job")
        .withLongOpt(JOB_CMD_DELETE_ARG)
        .create());
    group.addOption(OptionBuilder.withArgName("job-id")
        .hasArg()
        .withDescription("Show the parameters for a saved job")
        .withLongOpt(JOB_CMD_SHOW_ARG)
        .create());

    Option execOption = OptionBuilder.withArgName("job-id")
        .hasArg()
        .withDescription("Run a saved job")
        .withLongOpt(JOB_CMD_EXEC_ARG)
        .create();
    group.addOption(execOption);

    group.addOption(OptionBuilder
        .withDescription("List saved jobs")
        .withLongOpt(JOB_CMD_LIST_ARG)
        .create());

    relatedOpts.addOptionGroup(group);

    // Since the "common" options aren't used in the job tool,
    // add these settings here.
    relatedOpts.addOption(OptionBuilder
        .withDescription("Print more information while working")
        .withLongOpt(VERBOSE_ARG)
        .create());
    relatedOpts.addOption(OptionBuilder
        .withDescription("Print usage instructions")
        .withLongOpt(HELP_ARG)
        .create());

    return relatedOpts;
  }

  /**
   * @return RelatedOptions used by most/all Sqoop tools.
   */
  protected RelatedOptions getCommonOptions() {
    // Connection args (common)
    RelatedOptions commonOpts = new RelatedOptions("Common arguments");
    commonOpts.addOption(OptionBuilder.withArgName("jdbc-uri")
        .hasArg().withDescription("Specify JDBC connect string")
        .withLongOpt(CONNECT_STRING_ARG)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName("class-name")
        .hasArg().withDescription("Specify connection manager class name")
        .withLongOpt(CONN_MANAGER_CLASS_NAME)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName("properties-file")
        .hasArg().withDescription("Specify connection parameters file")
        .withLongOpt(CONNECT_PARAM_FILE)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName("class-name")
        .hasArg().withDescription("Manually specify JDBC driver class to use")
        .withLongOpt(DRIVER_ARG)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName("username")
        .hasArg().withDescription("Set authentication username")
        .withLongOpt(USERNAME_ARG)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName("password")
        .hasArg().withDescription("Set authentication password")
        .withLongOpt(PASSWORD_ARG)
        .create());
    commonOpts.addOption(OptionBuilder.withArgName(PASSWORD_PATH_ARG)
        .hasArg().withDescription("Set authentication password file path")
        .withLongOpt(PASSWORD_PATH_ARG)
        .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Read password from console")
        .create(PASSWORD_PROMPT_ARG));
    commonOpts.addOption(OptionBuilder.withArgName(PASSWORD_ALIAS_ARG)
      .hasArg().withDescription("Credential provider password alias")
      .withLongOpt(PASSWORD_ALIAS_ARG)
      .create());
    commonOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg().withDescription("Override $HADOOP_MAPRED_HOME_ARG")
        .withLongOpt(HADOOP_MAPRED_HOME_ARG)
        .create());

    commonOpts.addOption(OptionBuilder.withArgName("hdir")
            .hasArg().withDescription("Override $HADOOP_MAPRED_HOME_ARG")
            .withLongOpt(HADOOP_HOME_ARG)
            .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Skip copying jars to distributed cache")
        .withLongOpt(SKIP_DISTCACHE_ARG)
        .create());

    // misc (common)
    commonOpts.addOption(OptionBuilder
        .withDescription("Print more information while working")
        .withLongOpt(VERBOSE_ARG)
        .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Print usage instructions")
        .withLongOpt(HELP_ARG)
        .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Defines the temporary root directory for the import")
        .withLongOpt(TEMP_ROOTDIR_ARG)
        .hasArg()
        .withArgName("rootdir")
        .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Defines the transaction isolation level for metadata queries. "
            + "For more details check java.sql.Connection javadoc or the JDBC specificaiton")
        .withLongOpt(METADATA_TRANSACTION_ISOLATION_LEVEL)
        .hasArg()
        .withArgName("isolationlevel")
        .create());
    commonOpts.addOption(OptionBuilder
        .withDescription("Rethrow a RuntimeException on error occurred during the job")
        .withLongOpt(THROW_ON_ERROR_ARG)
        .create());
    // relax isolation requirements
    commonOpts.addOption(OptionBuilder
        .withDescription("Use read-uncommitted isolation for imports")
        .withLongOpt(RELAXED_ISOLATION)
        .create());

    commonOpts.addOption(OptionBuilder
        .withDescription("Disable the escaping mechanism of the Oracle/OraOop connection managers")
        .withLongOpt(ORACLE_ESCAPING_DISABLED)
        .hasArg()
        .withArgName("boolean")
        .create());

    return commonOpts;
  }

  /**
   * @param explicitHiveImport true if the user has an explicit --hive-import
   * available, or false if this is implied by the tool.
   * @return options governing interaction with Hive
   */
  protected RelatedOptions getHiveOptions(boolean explicitHiveImport) {
    RelatedOptions hiveOpts = new RelatedOptions("Hive arguments");
    if (explicitHiveImport) {
      hiveOpts.addOption(OptionBuilder
          .withDescription("Import tables into Hive "
          + "(Uses Hive's default delimiters if none are set.)")
          .withLongOpt(HIVE_IMPORT_ARG)
          .create());
    }

    hiveOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg().withDescription("Override $HIVE_HOME")
        .withLongOpt(HIVE_HOME_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder
        .withDescription("Overwrite existing data in the Hive table")
        .withLongOpt(HIVE_OVERWRITE_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder
        .withDescription("Fail if the target hive table exists")
        .withLongOpt(CREATE_HIVE_TABLE_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder.withArgName("table-name")
        .hasArg()
        .withDescription("Sets the table name to use when importing to hive")
        .withLongOpt(HIVE_TABLE_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder.withArgName("database-name")
      .hasArg()
      .withDescription("Sets the database name to use when importing to hive")
      .withLongOpt(HIVE_DATABASE_ARG)
      .create());
    hiveOpts.addOption(OptionBuilder
        .withDescription("Drop Hive record \\0x01 and row delimiters "
          + "(\\n\\r) from imported string fields")
        .withLongOpt(HIVE_DROP_DELIMS_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder
        .hasArg()
        .withDescription("Replace Hive record \\0x01 and row delimiters "
            + "(\\n\\r) from imported string fields with user-defined string")
        .withLongOpt(HIVE_DELIMS_REPLACEMENT_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder.withArgName("partition-key")
        .hasArg()
        .withDescription("Sets the partition key to use when importing to hive")
        .withLongOpt(HIVE_PARTITION_KEY_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder.withArgName("partition-value")
        .hasArg()
        .withDescription("Sets the partition value to use when importing "
            + "to hive")
        .withLongOpt(HIVE_PARTITION_VALUE_ARG)
        .create());
    hiveOpts.addOption(OptionBuilder.withArgName("hdfs path")
        .hasArg()
        .withDescription("Sets where the external table is in HDFS")
        .withLongOpt(HIVE_EXTERNAL_TABLE_LOCATION_ARG)
        .create());

    hiveOpts.addOption(OptionBuilder
        .hasArg()
        .withDescription("Override mapping for specific column to hive"
          + " types.")
        .withLongOpt(MAP_COLUMN_HIVE)
        .create());

    return hiveOpts;
  }

   /**
   * @return options governing interaction with HCatalog.
   */
  protected RelatedOptions getHCatalogOptions() {
    RelatedOptions hCatOptions = new RelatedOptions("HCatalog arguments");
    hCatOptions.addOption(OptionBuilder
      .hasArg()
      .withDescription("HCatalog table name")
      .withLongOpt(HCATALOG_TABLE_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder
      .hasArg()
      .withDescription("HCatalog database name")
      .withLongOpt(HCATALOG_DATABASE_ARG)
      .create());

    hCatOptions.addOption(OptionBuilder.withArgName("dir")
      .hasArg().withDescription("Override $HIVE_HOME")
      .withLongOpt(HIVE_HOME_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder.withArgName("hdir")
      .hasArg().withDescription("Override $HCAT_HOME")
      .withLongOpt(HCATALOG_HOME_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder.withArgName("partition-key")
      .hasArg()
      .withDescription("Sets the partition key to use when importing to hive")
      .withLongOpt(HIVE_PARTITION_KEY_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder.withArgName("partition-value")
      .hasArg()
      .withDescription("Sets the partition value to use when importing "
        + "to hive")
      .withLongOpt(HIVE_PARTITION_VALUE_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder
      .hasArg()
      .withDescription("Override mapping for specific column to hive"
        + " types.")
      .withLongOpt(MAP_COLUMN_HIVE)
      .create());
    hCatOptions.addOption(OptionBuilder.withArgName("partition-key")
      .hasArg()
      .withDescription("Sets the partition keys to use when importing to hive")
      .withLongOpt(HCATCALOG_PARTITION_KEYS_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder.withArgName("partition-value")
      .hasArg()
      .withDescription("Sets the partition values to use when importing "
        + "to hive")
      .withLongOpt(HCATALOG_PARTITION_VALUES_ARG)
      .create());
    return hCatOptions;
  }

  protected RelatedOptions getHCatImportOnlyOptions() {
    RelatedOptions hCatOptions = new RelatedOptions(
      "HCatalog import specific options");
    hCatOptions.addOption(OptionBuilder
      .withDescription("Create HCatalog before import")
      .withLongOpt(CREATE_HCATALOG_TABLE_ARG)
      .create());
    hCatOptions.addOption(OptionBuilder
      .withDescription("Drop and Create HCatalog before import")
      .withLongOpt(DROP_AND_CREATE_HCATALOG_TABLE)
      .create());
    hCatOptions.addOption(OptionBuilder
      .hasArg()
      .withDescription("HCatalog storage stanza for table creation")
      .withLongOpt(HCATALOG_STORAGE_STANZA_ARG)
      .create());
    return hCatOptions;
  }

  /**
   * @return options governing output format delimiters
   */
  protected RelatedOptions getOutputFormatOptions() {
    RelatedOptions formatOpts = new RelatedOptions(
        "Output line formatting arguments");
    formatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the field separator character")
        .withLongOpt(FIELDS_TERMINATED_BY_ARG)
        .create());
    formatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the end-of-line character")
        .withLongOpt(LINES_TERMINATED_BY_ARG)
        .create());
    formatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets a field enclosing character")
        .withLongOpt(OPTIONALLY_ENCLOSED_BY_ARG)
        .create());
    formatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets a required field enclosing character")
        .withLongOpt(ENCLOSED_BY_ARG)
        .create());
    formatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the escape character")
        .withLongOpt(ESCAPED_BY_ARG)
        .create());
    formatOpts.addOption(OptionBuilder
        .withDescription("Uses MySQL's default delimiter set: "
          + "fields: ,  lines: \\n  escaped-by: \\  optionally-enclosed-by: '")
        .withLongOpt(MYSQL_DELIMITERS_ARG)
        .create());

    return formatOpts;
  }

  /**
   * @return options governing input format delimiters.
   */
  protected RelatedOptions getInputFormatOptions() {
    RelatedOptions inputFormatOpts =
        new RelatedOptions("Input parsing arguments");
    inputFormatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the input field separator")
        .withLongOpt(INPUT_FIELDS_TERMINATED_BY_ARG)
        .create());
    inputFormatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the input end-of-line char")
        .withLongOpt(INPUT_LINES_TERMINATED_BY_ARG)
        .create());
    inputFormatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets a field enclosing character")
        .withLongOpt(INPUT_OPTIONALLY_ENCLOSED_BY_ARG)
        .create());
    inputFormatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets a required field encloser")
        .withLongOpt(INPUT_ENCLOSED_BY_ARG)
        .create());
    inputFormatOpts.addOption(OptionBuilder.withArgName("char")
        .hasArg()
        .withDescription("Sets the input escape character")
        .withLongOpt(INPUT_ESCAPED_BY_ARG)
        .create());

    return inputFormatOpts;
  }

  /**
   * @param multiTable true if these options will be used for bulk code-gen.
   * @return options related to code generation.
   */
  protected RelatedOptions getCodeGenOpts(boolean multiTable) {
    RelatedOptions codeGenOpts =
        new RelatedOptions("Code generation arguments");
    codeGenOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg()
        .withDescription("Output directory for generated code")
        .withLongOpt(CODE_OUT_DIR_ARG)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg()
        .withDescription("Output directory for compiled objects")
        .withLongOpt(BIN_OUT_DIR_ARG)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("name")
        .hasArg()
        .withDescription("Put auto-generated classes in this package")
        .withLongOpt(PACKAGE_NAME_ARG)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("null-str")
        .hasArg()
        .withDescription("Null string representation")
        .withLongOpt(NULL_STRING)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("null-str")
        .hasArg()
        .withDescription("Input null string representation")
        .withLongOpt(INPUT_NULL_STRING)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("null-str")
        .hasArg()
        .withDescription("Null non-string representation")
        .withLongOpt(NULL_NON_STRING)
        .create());
    codeGenOpts.addOption(OptionBuilder.withArgName("null-str")
        .hasArg()
        .withDescription("Input null non-string representation")
        .withLongOpt(INPUT_NULL_NON_STRING)
        .create());
    codeGenOpts.addOption(OptionBuilder
        .hasArg()
        .withDescription("Override mapping for specific columns to java types")
        .withLongOpt(MAP_COLUMN_JAVA)
        .create());
    codeGenOpts.addOption(OptionBuilder
        .hasArg()
        .withDescription("Disable special characters escaping in column names")
        .withLongOpt(ESCAPE_MAPPING_COLUMN_NAMES_ENABLED)
        .withArgName("boolean")
        .create());

    if (!multiTable) {
      codeGenOpts.addOption(OptionBuilder.withArgName("name")
          .hasArg()
          .withDescription("Sets the generated class name. "
          + "This overrides --" + PACKAGE_NAME_ARG + ". When combined "
          + "with --" + JAR_FILE_NAME_ARG + ", sets the input class.")
          .withLongOpt(CLASS_NAME_ARG)
          .create());
    }
    return codeGenOpts;
  }

  protected RelatedOptions getHBaseOptions() {
    RelatedOptions hbaseOpts =
        new RelatedOptions("HBase arguments");
    hbaseOpts.addOption(OptionBuilder.withArgName("table")
        .hasArg()
        .withDescription("Import to <table> in HBase")
        .withLongOpt(HBASE_TABLE_ARG)
        .create());
    hbaseOpts.addOption(OptionBuilder.withArgName("family")
        .hasArg()
        .withDescription("Sets the target column family for the import")
        .withLongOpt(HBASE_COL_FAM_ARG)
        .create());
    hbaseOpts.addOption(OptionBuilder.withArgName("col")
        .hasArg()
        .withDescription("Specifies which input column to use as the row key")
        .withLongOpt(HBASE_ROW_KEY_ARG)
        .create());
    hbaseOpts.addOption(OptionBuilder
        .withDescription("Enables HBase bulk loading")
        .withLongOpt(HBASE_BULK_LOAD_ENABLED_ARG)
        .create());
    hbaseOpts.addOption(OptionBuilder
        .withDescription("If specified, create missing HBase tables")
        .withLongOpt(HBASE_CREATE_TABLE_ARG)
        .create());

    return hbaseOpts;
  }

  protected RelatedOptions getAccumuloOptions() {
    RelatedOptions accumuloOpts =
      new RelatedOptions("Accumulo arguments");
    accumuloOpts.addOption(OptionBuilder.withArgName("table")
      .hasArg()
      .withDescription("Import to <table> in Accumulo")
      .withLongOpt(ACCUMULO_TABLE_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("family")
      .hasArg()
      .withDescription("Sets the target column family for the import")
      .withLongOpt(ACCUMULO_COL_FAM_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("col")
      .hasArg()
      .withDescription("Specifies which input column to use as the row key")
      .withLongOpt(ACCUMULO_ROW_KEY_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("vis")
      .hasArg()
      .withDescription("Visibility token to be applied to all rows imported")
      .withLongOpt(ACCUMULO_VISIBILITY_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder
      .withDescription("If specified, create missing Accumulo tables")
      .withLongOpt(ACCUMULO_CREATE_TABLE_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("size")
      .hasArg()
      .withDescription("Batch size in bytes")
      .withLongOpt(ACCUMULO_BATCH_SIZE_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("latency")
      .hasArg()
      .withDescription("Max write latency in milliseconds")
      .withLongOpt(ACCUMULO_MAX_LATENCY_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("zookeepers")
      .hasArg()
      .withDescription("Comma-separated list of zookeepers (host:port)")
      .withLongOpt(ACCUMULO_ZOOKEEPERS_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("instance")
      .hasArg()
      .withDescription("Accumulo instance name.")
      .withLongOpt(ACCUMULO_INSTANCE_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("user")
      .hasArg()
      .withDescription("Accumulo user name.")
      .withLongOpt(ACCUMULO_USER_ARG)
      .create());
    accumuloOpts.addOption(OptionBuilder.withArgName("password")
      .hasArg()
      .withDescription("Accumulo password.")
      .withLongOpt(ACCUMULO_PASSWORD_ARG)
      .create());

    return accumuloOpts;
  }

  protected void applyAccumuloOptions(CommandLine in, SqoopOptions out) {
    if (in.hasOption(ACCUMULO_TABLE_ARG)) {
      out.setAccumuloTable(in.getOptionValue(ACCUMULO_TABLE_ARG));
    }

    if (in.hasOption(ACCUMULO_COL_FAM_ARG)) {
      out.setAccumuloColFamily(in.getOptionValue(ACCUMULO_COL_FAM_ARG));
    }

    if (in.hasOption(ACCUMULO_ROW_KEY_ARG)) {
      out.setAccumuloRowKeyColumn(in.getOptionValue(ACCUMULO_ROW_KEY_ARG));
    }

    if (in.hasOption(ACCUMULO_VISIBILITY_ARG)) {
      out.setAccumuloVisibility(in.getOptionValue(ACCUMULO_VISIBILITY_ARG));
    }

    if (in.hasOption(ACCUMULO_CREATE_TABLE_ARG)) {
      out.setCreateAccumuloTable(true);
    }

    if (in.hasOption(ACCUMULO_BATCH_SIZE_ARG)) {
      out.setAccumuloBatchSize(Long.parseLong(
        in.getOptionValue(ACCUMULO_BATCH_SIZE_ARG)));
    }

    if (in.hasOption(ACCUMULO_MAX_LATENCY_ARG)) {
      out.setAccumuloMaxLatency(Long.parseLong(
        in.getOptionValue(ACCUMULO_MAX_LATENCY_ARG)));
    }

    if (in.hasOption(ACCUMULO_ZOOKEEPERS_ARG)) {
      out.setAccumuloZookeepers(in.getOptionValue(ACCUMULO_ZOOKEEPERS_ARG));
    }

    if (in.hasOption(ACCUMULO_INSTANCE_ARG)) {
      out.setAccumuloInstance(in.getOptionValue(ACCUMULO_INSTANCE_ARG));
    }

    if (in.hasOption(ACCUMULO_USER_ARG)) {
      out.setAccumuloUser(in.getOptionValue(ACCUMULO_USER_ARG));
    }

    if (in.hasOption(ACCUMULO_PASSWORD_ARG)) {
      out.setAccumuloPassword(in.getOptionValue(ACCUMULO_PASSWORD_ARG));
    }
  }


  @SuppressWarnings("static-access")
  protected void addValidationOpts(RelatedOptions validationOptions) {
    validationOptions.addOption(OptionBuilder
      .withDescription("Validate the copy using the configured validator")
      .withLongOpt(VALIDATE_ARG)
      .create());
    validationOptions.addOption(OptionBuilder
      .withArgName(VALIDATOR_CLASS_ARG).hasArg()
      .withDescription("Fully qualified class name for the Validator")
      .withLongOpt(VALIDATOR_CLASS_ARG)
      .create());
    validationOptions.addOption(OptionBuilder
      .withArgName(VALIDATION_THRESHOLD_CLASS_ARG).hasArg()
      .withDescription("Fully qualified class name for ValidationThreshold")
      .withLongOpt(VALIDATION_THRESHOLD_CLASS_ARG)
      .create());
    validationOptions.addOption(OptionBuilder
      .withArgName(VALIDATION_FAILURE_HANDLER_CLASS_ARG).hasArg()
      .withDescription("Fully qualified class name for "
        + "ValidationFailureHandler")
      .withLongOpt(VALIDATION_FAILURE_HANDLER_CLASS_ARG)
      .create());
  }

  /**
   * Apply common command-line to the state.
   */
  protected void applyCommonOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    // common options.
    if (in.hasOption(VERBOSE_ARG)) {
      // Immediately switch into DEBUG logging.
      out.setVerbose(true);
      LoggingUtils.setDebugLevel();
      LOG.debug("Enabled debug logging.");
    }

    if (in.hasOption(HELP_ARG)) {
      ToolOptions toolOpts = new ToolOptions();
      configureOptions(toolOpts);
      printHelp(toolOpts);
      throw new InvalidOptionsException("");
    }

    if (in.hasOption(TEMP_ROOTDIR_ARG)) {
      out.setTempRootDir(in.getOptionValue(TEMP_ROOTDIR_ARG));
    }

    if (in.hasOption(THROW_ON_ERROR_ARG)) {
      LOG.debug("Throw exception on error during job is enabled.");
      out.setThrowOnError(true);
    }

    if (in.hasOption(CONNECT_STRING_ARG)) {
      out.setConnectString(in.getOptionValue(CONNECT_STRING_ARG));
    }

    if (in.hasOption(CONN_MANAGER_CLASS_NAME)) {
        out.setConnManagerClassName(in.getOptionValue(CONN_MANAGER_CLASS_NAME));
    }

    if (in.hasOption(CONNECT_PARAM_FILE)) {
      File paramFile = new File(in.getOptionValue(CONNECT_PARAM_FILE));
      if (!paramFile.exists()) {
        throw new InvalidOptionsException(
                "Specified connection parameter file not found: " + paramFile);
      }
      InputStream inStream = null;
      Properties connectionParams = new Properties();
      try {
        inStream = new FileInputStream(
                      new File(in.getOptionValue(CONNECT_PARAM_FILE)));
        connectionParams.load(inStream);
      } catch (IOException ex) {
        LOG.warn("Failed to load connection parameter file", ex);
        throw new InvalidOptionsException(
                "Error while loading connection parameter file: "
                + ex.getMessage());
      } finally {
        if (inStream != null) {
          try {
            inStream.close();
          } catch (IOException ex) {
            LOG.warn("Failed to close input stream", ex);
          }
        }
      }
      LOG.debug("Loaded connection parameters: " + connectionParams);
      out.setConnectionParams(connectionParams);
    }

    if (in.hasOption(NULL_STRING)) {
        out.setNullStringValue(in.getOptionValue(NULL_STRING));
    }

    if (in.hasOption(INPUT_NULL_STRING)) {
        out.setInNullStringValue(in.getOptionValue(INPUT_NULL_STRING));
    }

    if (in.hasOption(NULL_NON_STRING)) {
        out.setNullNonStringValue(in.getOptionValue(NULL_NON_STRING));
    }

    if (in.hasOption(INPUT_NULL_NON_STRING)) {
        out.setInNullNonStringValue(in.getOptionValue(INPUT_NULL_NON_STRING));
    }

    if (in.hasOption(DRIVER_ARG)) {
      out.setDriverClassName(in.getOptionValue(DRIVER_ARG));
    }

    if (in.hasOption(SKIP_DISTCACHE_ARG)) {
      LOG.debug("Disabling dist cache");
      out.setSkipDistCache(true);
    }

    applyCredentialsOptions(in, out);


    if (in.hasOption(HADOOP_MAPRED_HOME_ARG)) {
      out.setHadoopMapRedHome(in.getOptionValue(HADOOP_MAPRED_HOME_ARG));
      // Only consider HADOOP_HOME if HADOOP_MAPRED_HOME is not set
    } else if (in.hasOption(HADOOP_HOME_ARG)) {
        out.setHadoopMapRedHome(in.getOptionValue(HADOOP_HOME_ARG));
    }
    if (in.hasOption(RELAXED_ISOLATION)) {
      out.setRelaxedIsolation(true);
    }

    if (in.hasOption(METADATA_TRANSACTION_ISOLATION_LEVEL)) {
      String transactionLevel = in.getOptionValue(METADATA_TRANSACTION_ISOLATION_LEVEL);
      try {
        out.setMetadataTransactionIsolationLevel(JDBCTransactionLevels.valueOf(transactionLevel).getTransactionIsolationLevelValue());
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Only transaction isolation levels defined by "
            + "java.sql.Connection class are supported. Check the "
            + "java.sql.Connection javadocs for more details", e);
      }
    }

    if (in.hasOption(ORACLE_ESCAPING_DISABLED)) {
      out.setOracleEscapingDisabled(Boolean.parseBoolean(in.getOptionValue(ORACLE_ESCAPING_DISABLED)));
    }

    if (in.hasOption(ESCAPE_MAPPING_COLUMN_NAMES_ENABLED)) {
      out.setEscapeMappingColumnNamesEnabled(Boolean.parseBoolean(in.getOptionValue(
          ESCAPE_MAPPING_COLUMN_NAMES_ENABLED)));
    }
  }

  private void applyCredentialsOptions(CommandLine in, SqoopOptions out)
    throws InvalidOptionsException {
    if (in.hasOption(USERNAME_ARG)) {
      out.setUsername(in.getOptionValue(USERNAME_ARG));
      if (null == out.getPassword()) {
        // Set password to empty if the username is set first,
        // to ensure that they're either both null or neither is.
        out.setPassword("");
      }
    }

    if (in.hasOption(PASSWORD_ARG)) {
      LOG.warn("Setting your password on the command-line is insecure. "
          + "Consider using -" + PASSWORD_PROMPT_ARG + " instead.");
      out.setPassword(in.getOptionValue(PASSWORD_ARG));
    }

    if (in.hasOption(PASSWORD_PROMPT_ARG)) {
      out.setPasswordFromConsole();
    }

    if (in.hasOption(PASSWORD_PATH_ARG)) {
      if (in.hasOption(PASSWORD_ARG) || in.hasOption(PASSWORD_PROMPT_ARG)
          || in.hasOption(PASSWORD_ALIAS_ARG)) {
        throw new InvalidOptionsException("Only one of password, password "
          + "alias or path to a password file must be specified.");
      }

      try {
        out.setPasswordFilePath(in.getOptionValue(PASSWORD_PATH_ARG));
        // apply password from file into password in options
        out.setPassword(CredentialsUtil.fetchPassword(out));
        // And allow the PasswordLoader to clean up any sensitive properties
        CredentialsUtil.cleanUpSensitiveProperties(out.getConf());
      } catch (IOException ex) {
        LOG.warn("Failed to load password file", ex);
        throw (InvalidOptionsException)
          new InvalidOptionsException("Error while loading password file: "
            + ex.getMessage()).initCause(ex);
      }
    }
    if (in.hasOption(PASSWORD_ALIAS_ARG)) {
      if (in.hasOption(PASSWORD_ARG) || in.hasOption(PASSWORD_PROMPT_ARG)
          || in.hasOption(PASSWORD_PATH_ARG)) {
        throw new InvalidOptionsException("Only one of password, password "
          + "alias or path to a password file must be specified.");
      }
      out.setPasswordAlias(in.getOptionValue(PASSWORD_ALIAS_ARG));
      if (!CredentialProviderHelper.isProviderAvailable()) {
        throw new InvalidOptionsException(
          "CredentialProvider facility not available in the hadoop "
          + " environment used");
      }
      try {
        out.setPassword(CredentialProviderHelper
          .resolveAlias(out.getConf(), in.getOptionValue(PASSWORD_ALIAS_ARG)));
      } catch (IOException ioe) {
        throw (InvalidOptionsException)
          new InvalidOptionsException("Unable to process alias")
            .initCause(ioe);
      }
    }
  }

  protected void applyHiveOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    if (in.hasOption(HIVE_HOME_ARG)) {
      out.setHiveHome(in.getOptionValue(HIVE_HOME_ARG));
    }

    if (in.hasOption(HIVE_IMPORT_ARG)) {
      out.setHiveImport(true);
    }

    if (in.hasOption(HIVE_OVERWRITE_ARG)) {
      out.setOverwriteHiveTable(true);
    }

    if (in.hasOption(CREATE_HIVE_TABLE_ARG)) {
      out.setFailIfHiveTableExists(true);
    }

    if (in.hasOption(HIVE_TABLE_ARG)) {
      out.setHiveTableName(in.getOptionValue(HIVE_TABLE_ARG));
    }

    if (in.hasOption(HIVE_DATABASE_ARG)) {
      out.setHiveDatabaseName(in.getOptionValue(HIVE_DATABASE_ARG));
    }

    if (in.hasOption(HIVE_DROP_DELIMS_ARG)) {
      out.setHiveDropDelims(true);
    }

    if (in.hasOption(HIVE_DELIMS_REPLACEMENT_ARG)) {
      out.setHiveDelimsReplacement(
              in.getOptionValue(HIVE_DELIMS_REPLACEMENT_ARG));
    }

    if (in.hasOption(HIVE_PARTITION_KEY_ARG)) {
      out.setHivePartitionKey(in.getOptionValue(HIVE_PARTITION_KEY_ARG));
    }

    if (in.hasOption(HIVE_PARTITION_VALUE_ARG)) {
      out.setHivePartitionValue(in.getOptionValue(HIVE_PARTITION_VALUE_ARG));
    }

   if (in.hasOption(MAP_COLUMN_HIVE)) {
      out.setMapColumnHive(in.getOptionValue(MAP_COLUMN_HIVE));
   }
   if (in.hasOption(HIVE_EXTERNAL_TABLE_LOCATION_ARG)) {
     out.setHiveExternalTableDir(in.getOptionValue(HIVE_EXTERNAL_TABLE_LOCATION_ARG));
   }

  }

  protected void applyHCatalogOptions(CommandLine in, SqoopOptions out) {
    if (in.hasOption(HCATALOG_TABLE_ARG)) {
      out.setHCatTableName(in.getOptionValue(HCATALOG_TABLE_ARG));
    }

    if (in.hasOption(HCATALOG_DATABASE_ARG)) {
      out.setHCatDatabaseName(in.getOptionValue(HCATALOG_DATABASE_ARG));
    }

    if (in.hasOption(HCATALOG_STORAGE_STANZA_ARG)) {
      out.setHCatStorageStanza(in.getOptionValue(HCATALOG_STORAGE_STANZA_ARG));
    }

    if (in.hasOption(CREATE_HCATALOG_TABLE_ARG)) {
      out.setCreateHCatalogTable(true);
    }

    if (in.hasOption(DROP_AND_CREATE_HCATALOG_TABLE)) {
      out.setDropAndCreateHCatalogTable(true);
    }

    if (in.hasOption(HCATALOG_HOME_ARG)) {
      out.setHCatHome(in.getOptionValue(HCATALOG_HOME_ARG));
    }

    // Allow some of the hive options also

    if (in.hasOption(HIVE_HOME_ARG)) {
      out.setHiveHome(in.getOptionValue(HIVE_HOME_ARG));
    }

    if (in.hasOption(HCATCALOG_PARTITION_KEYS_ARG)) {
      out.setHCatalogPartitionKeys(
        in.getOptionValue(HCATCALOG_PARTITION_KEYS_ARG));
    }

    if (in.hasOption(HCATALOG_PARTITION_VALUES_ARG)) {
      out.setHCatalogPartitionValues(
        in.getOptionValue(HCATALOG_PARTITION_VALUES_ARG));
    }

    if (in.hasOption(HIVE_PARTITION_KEY_ARG)) {
      out.setHivePartitionKey(in.getOptionValue(HIVE_PARTITION_KEY_ARG));
    }

    if (in.hasOption(HIVE_PARTITION_VALUE_ARG)) {
      out.setHivePartitionValue(in.getOptionValue(HIVE_PARTITION_VALUE_ARG));
    }

    if (in.hasOption(MAP_COLUMN_HIVE)) {
      out.setMapColumnHive(in.getOptionValue(MAP_COLUMN_HIVE));
    }

  }


  protected void applyOutputFormatOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    if (in.hasOption(FIELDS_TERMINATED_BY_ARG)) {
      out.setFieldsTerminatedBy(SqoopOptions.toChar(
          in.getOptionValue(FIELDS_TERMINATED_BY_ARG)));
      out.setExplicitOutputDelims(true);
    }

    if (in.hasOption(LINES_TERMINATED_BY_ARG)) {
      out.setLinesTerminatedBy(SqoopOptions.toChar(
          in.getOptionValue(LINES_TERMINATED_BY_ARG)));
      out.setExplicitOutputDelims(true);
    }

    if (in.hasOption(OPTIONALLY_ENCLOSED_BY_ARG)) {
      out.setEnclosedBy(SqoopOptions.toChar(
          in.getOptionValue(OPTIONALLY_ENCLOSED_BY_ARG)));
      out.setOutputEncloseRequired(false);
      out.setExplicitOutputDelims(true);
    }

    if (in.hasOption(ENCLOSED_BY_ARG)) {
      out.setEnclosedBy(SqoopOptions.toChar(
          in.getOptionValue(ENCLOSED_BY_ARG)));
      out.setOutputEncloseRequired(true);
      out.setExplicitOutputDelims(true);
    }

    if (in.hasOption(ESCAPED_BY_ARG)) {
      out.setEscapedBy(SqoopOptions.toChar(
          in.getOptionValue(ESCAPED_BY_ARG)));
      out.setExplicitOutputDelims(true);
    }

    if (in.hasOption(MYSQL_DELIMITERS_ARG)) {
      out.setOutputEncloseRequired(false);
      out.setFieldsTerminatedBy(',');
      out.setLinesTerminatedBy('\n');
      out.setEscapedBy('\\');
      out.setEnclosedBy('\'');
      out.setExplicitOutputDelims(true);
    }
  }

  protected void applyInputFormatOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    if (in.hasOption(INPUT_FIELDS_TERMINATED_BY_ARG)) {
      out.setInputFieldsTerminatedBy(SqoopOptions.toChar(
          in.getOptionValue(INPUT_FIELDS_TERMINATED_BY_ARG)));
      out.setExplicitInputDelims(true);
    }

    if (in.hasOption(INPUT_LINES_TERMINATED_BY_ARG)) {
      out.setInputLinesTerminatedBy(SqoopOptions.toChar(
          in.getOptionValue(INPUT_LINES_TERMINATED_BY_ARG)));
      out.setExplicitInputDelims(true);
    }

    if (in.hasOption(INPUT_OPTIONALLY_ENCLOSED_BY_ARG)) {
      out.setInputEnclosedBy(SqoopOptions.toChar(
          in.getOptionValue(INPUT_OPTIONALLY_ENCLOSED_BY_ARG)));
      out.setInputEncloseRequired(false);
      out.setExplicitInputDelims(true);
    }

    if (in.hasOption(INPUT_ENCLOSED_BY_ARG)) {
      out.setInputEnclosedBy(SqoopOptions.toChar(
          in.getOptionValue(INPUT_ENCLOSED_BY_ARG)));
      out.setInputEncloseRequired(true);
      out.setExplicitInputDelims(true);
    }

    if (in.hasOption(INPUT_ESCAPED_BY_ARG)) {
      out.setInputEscapedBy(SqoopOptions.toChar(
          in.getOptionValue(INPUT_ESCAPED_BY_ARG)));
      out.setExplicitInputDelims(true);
    }
  }

  protected void applyCodeGenOptions(CommandLine in, SqoopOptions out,
      boolean multiTable) throws InvalidOptionsException {
    if (in.hasOption(CODE_OUT_DIR_ARG)) {
      out.setCodeOutputDir(in.getOptionValue(CODE_OUT_DIR_ARG));
    }

    if (in.hasOption(BIN_OUT_DIR_ARG)) {
      out.setJarOutputDir(in.getOptionValue(BIN_OUT_DIR_ARG));
    }

    if (in.hasOption(PACKAGE_NAME_ARG)) {
      out.setPackageName(in.getOptionValue(PACKAGE_NAME_ARG));
    }

    if (in.hasOption(MAP_COLUMN_JAVA)) {
      out.setMapColumnJava(in.getOptionValue(MAP_COLUMN_JAVA));
    }

    if (!multiTable && in.hasOption(CLASS_NAME_ARG)) {
      out.setClassName(in.getOptionValue(CLASS_NAME_ARG));
    }

    if (in.hasOption(ESCAPE_MAPPING_COLUMN_NAMES_ENABLED)) {
      out.setEscapeMappingColumnNamesEnabled(Boolean.parseBoolean(in.getOptionValue(
          ESCAPE_MAPPING_COLUMN_NAMES_ENABLED)));
    }
  }

  protected void applyHBaseOptions(CommandLine in, SqoopOptions out) {
    if (in.hasOption(HBASE_TABLE_ARG)) {
      out.setHBaseTable(in.getOptionValue(HBASE_TABLE_ARG));
    }

    if (in.hasOption(HBASE_COL_FAM_ARG)) {
      out.setHBaseColFamily(in.getOptionValue(HBASE_COL_FAM_ARG));
    }

    if (in.hasOption(HBASE_ROW_KEY_ARG)) {
      out.setHBaseRowKeyColumn(in.getOptionValue(HBASE_ROW_KEY_ARG));
    }

    out.setHBaseBulkLoadEnabled(in.hasOption(HBASE_BULK_LOAD_ENABLED_ARG));

    if (in.hasOption(HBASE_CREATE_TABLE_ARG)) {
      out.setCreateHBaseTable(true);
    }
  }

  protected void applyValidationOptions(CommandLine in, SqoopOptions out)
    throws InvalidOptionsException {
    if (in.hasOption(VALIDATE_ARG)) {
      out.setValidationEnabled(true);
    }

    // Class Names are converted to Class in light of failing early
    if (in.hasOption(VALIDATOR_CLASS_ARG)) {
      out.setValidatorClass(
        getClassByName(in.getOptionValue(VALIDATOR_CLASS_ARG)));
    }

    if (in.hasOption(VALIDATION_THRESHOLD_CLASS_ARG)) {
      out.setValidationThresholdClass(
        getClassByName(in.getOptionValue(VALIDATION_THRESHOLD_CLASS_ARG)));
    }

    if (in.hasOption(VALIDATION_FAILURE_HANDLER_CLASS_ARG)) {
      out.setValidationFailureHandlerClass(getClassByName(
        in.getOptionValue(VALIDATION_FAILURE_HANDLER_CLASS_ARG)));
    }
  }

  protected Class<?> getClassByName(String className)
    throws InvalidOptionsException {
    try {
      return Class.forName(className, true,
        Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException e) {
      throw new InvalidOptionsException(e.getMessage());
    }
  }

  protected void validateCommonOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.getConnectString() == null) {
      throw new InvalidOptionsException(
          "Error: Required argument --connect is missing."
          + HELP_STR);
    }
  }

  protected void validateCodeGenOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.getClassName() != null && options.getPackageName() != null) {
      throw new InvalidOptionsException(
          "--class-name overrides --package-name. You cannot use both."
          + HELP_STR);
    }
  }

  protected void validateOutputFormatOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.doHiveImport()) {
      if (!options.explicitOutputDelims()) {
        // user hasn't manually specified delimiters, and wants to import
        // straight to Hive. Use Hive-style delimiters.
        LOG.info("Using Hive-specific delimiters for output. You can override");
        LOG.info("delimiters with --fields-terminated-by, etc.");
        options.setOutputDelimiters(DelimiterSet.HIVE_DELIMITERS);
      }

      if (options.getOutputEscapedBy() != DelimiterSet.NULL_CHAR) {
        LOG.warn("Hive does not support escape characters in fields;");
        LOG.warn("parse errors in Hive may result from using --escaped-by.");
      }

      if (options.getOutputEnclosedBy() != DelimiterSet.NULL_CHAR) {
        LOG.warn("Hive does not support quoted strings; parse errors");
        LOG.warn("in Hive may result from using --enclosed-by.");
      }
    }
  }

  protected void validateHiveOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.getHiveDelimsReplacement() != null
            && options.doHiveDropDelims()) {
      throw new InvalidOptionsException("The " + HIVE_DROP_DELIMS_ARG
              + " option conflicts with the " + HIVE_DELIMS_REPLACEMENT_ARG
              + " option." + HELP_STR);
    }

    // Make sure that one of hCatalog or hive jobs are used
    String hCatTable = options.getHCatTableName();
    if (hCatTable != null && options.doHiveImport()) {
      throw new InvalidOptionsException("The " + HCATALOG_TABLE_ARG
        + " option conflicts with the " + HIVE_IMPORT_ARG
        + " option." + HELP_STR);
    }

    if (options.doHiveImport()
        && options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
      throw new InvalidOptionsException("Hive import is not compatible with "
        + "importing into AVRO format.");
    }

    if (options.doHiveImport()
        && options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
      throw new InvalidOptionsException("Hive import is not compatible with "
        + "importing into SequenceFile format.");
    }

    // Hive import and create hive table not compatible for ParquetFile format
    if (options.doHiveImport()
        && options.doFailIfHiveTableExists()
        && options.getFileLayout() == SqoopOptions.FileLayout.ParquetFile) {
      throw new InvalidOptionsException("Hive import and create hive table is not compatible with "
        + "importing into ParquetFile format.");
      }

    if (options.doHiveImport()
        && options.getIncrementalMode().equals(IncrementalMode.DateLastModified)) {
      throw new InvalidOptionsException(HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED);
    }

    if (options.doHiveImport()
        && options.isAppendMode()
        && !options.getIncrementalMode().equals(IncrementalMode.AppendRows)) {
      throw new InvalidOptionsException("Append mode for hive imports is not "
          + " yet supported. Please remove the parameter --append-mode");
    }

    // Many users are reporting issues when they are trying to import data
    // directly into hive warehouse. This should prevent users from doing
    // so in case of a default location.
    String defaultHiveWarehouse = "/user/hive/warehouse";
    if (options.doHiveImport()
      && ((
        options.getWarehouseDir() != null
        && options.getWarehouseDir().startsWith(defaultHiveWarehouse)
        ) || (
        options.getTargetDir() != null
        && options.getTargetDir().startsWith(defaultHiveWarehouse)
    ))) {
      LOG.warn("It seems that you're doing hive import directly into default");
      LOG.warn("hive warehouse directory which is not supported. Sqoop is");
      LOG.warn("firstly importing data into separate directory and then");
      LOG.warn("inserting data into hive. Please consider removing");
      LOG.warn("--target-dir or --warehouse-dir into /user/hive/warehouse in");
      LOG.warn("case that you will detect any issues.");
    }

    // Warn about using hive specific arguments without hive import itself
    // In HCatalog support some of the Hive options are reused
    if (!options.doHiveImport()
      && (((options.getHiveHome() != null
        && !options.getHiveHome().
          equals(SqoopOptions.getHiveHomeDefault())
          && hCatTable == null))
      || options.doOverwriteHiveTable()
      || options.doFailIfHiveTableExists()
      || (options.getHiveTableName() != null
        && !options.getHiveTableName().equals(options.getTableName()))
        || (options.getHivePartitionKey() != null && hCatTable == null)
        || (options.getHivePartitionValue() != null && hCatTable == null)
        || (options.getMapColumnHive().size() > 0 && hCatTable == null))) {
      LOG.warn("It seems that you've specified at least one of following:");
      LOG.warn("\t--hive-home");
      LOG.warn("\t--hive-overwrite");
      LOG.warn("\t--create-hive-table");
      LOG.warn("\t--hive-table");
      LOG.warn("\t--hive-partition-key");
      LOG.warn("\t--hive-partition-value");
      LOG.warn("\t--map-column-hive");
      LOG.warn("Without specifying parameter --hive-import. Please note that");
      LOG.warn("those arguments will not be used in this session. Either");
      LOG.warn("specify --hive-import to apply them correctly or remove them");
      LOG.warn("from command line to remove this warning.");
      LOG.info("Please note that --hive-home, --hive-partition-key, ");
      LOG.info("\t hive-partition-value and --map-column-hive options are ");
      LOG.info("\t are also valid for HCatalog imports and exports");
    }
    // importing to Hive external tables requires target directory to be set
    // for external table's location
    Boolean isNotHiveImportButExternalTableDirIsSet = !options.doHiveImport() && !org.apache.commons.lang.StringUtils.isBlank(options.getHiveExternalTableDir());
    if (isNotHiveImportButExternalTableDirIsSet) {
      LOG.warn("Importing to external Hive table requires --hive-import parameter to be set");
      throw new InvalidOptionsException("Importing to external Hive table requires --hive-import parameter to be set."
          + HELP_STR);
    }
  }

  protected void validateAccumuloOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if ((options.getAccumuloColFamily() != null
        && options.getAccumuloTable() == null)
        || (options.getAccumuloColFamily() == null
        && options.getAccumuloTable() != null)) {
      throw new InvalidOptionsException(
          "Both --accumulo-table and --accumulo-column-family must be set."
          + HELP_STR);
    }

    if (options.getAccumuloTable() != null
        && options.getHBaseTable() != null) {
      throw new InvalidOptionsException("HBase import is incompatible with "
            + "Accumulo import.");
    }
    if (options.getAccumuloTable() != null
        && options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      throw new InvalidOptionsException("Accumulo import is not compatible "
        + "with importing into file format.");
    }
    if (options.getAccumuloTable() != null
        && options.getHBaseColFamily() != null) {
      throw new InvalidOptionsException("Use --accumulo-column-family with "
            + "Accumulo import.");
    }
    if (options.getAccumuloTable() != null
        && options.getAccumuloUser() == null) {
      throw
        new InvalidOptionsException("Must specify Accumulo user.");
    }
    if (options.getAccumuloTable() != null
        && options.getAccumuloInstance() == null) {
      throw new
        InvalidOptionsException("Must specify Accumulo instance.");
    }
    if (options.getAccumuloTable() != null
        && options.getAccumuloZookeepers() == null) {
      throw new
        InvalidOptionsException("Must specify Zookeeper server(s).");
    }
  }

  protected void validateHCatalogOptions(SqoopOptions options)
    throws InvalidOptionsException {
    // Make sure that one of hCatalog or hive jobs are used
    String hCatTable = options.getHCatTableName();
    if (hCatTable == null) {
      if (options.getHCatHome() != null && !options.getHCatHome().
        equals(SqoopOptions.getHCatHomeDefault())) {
        LOG.warn("--hcatalog-home option will be ignored in "
          + "non-HCatalog jobs");
      }
      if (options.getHCatDatabaseName() != null) {
        LOG.warn("--hcatalog-database option will be ignored  "
          + "without --hcatalog-table");
      }

      if (options.getHCatStorageStanza() != null) {
        LOG.warn("--hcatalog-storage-stanza option will be ignored "
          + "without --hatalog-table");
      }
      return;
    }
    if(isSet(options.getHCatTableName()) && SqoopHCatUtilities.isHCatView(options)){
      throw  new InvalidOptionsException("Reads/Writes from and to Views are not supported by HCatalog");
    }

    if (options.explicitInputDelims()) {
      LOG.warn("Input field/record delimiter options are not "
        + "used in HCatalog jobs unless the format is text.   It is better "
        + "to use --hive-import in those cases.  For text formats");
    }
    if (options.explicitOutputDelims()
      || options.getHiveDelimsReplacement() != null
      || options.doHiveDropDelims()) {
      LOG.warn("Output field/record delimiter options are not useful"
        + " in HCatalog jobs for most of the output types except text based "
        + " formats is text. It is better "
        + "to use --hive-import in those cases.  For non text formats, ");
    }
    if (options.doHiveImport()) {
      throw new InvalidOptionsException("The " + HCATALOG_TABLE_ARG
        + " option conflicts with the " + HIVE_IMPORT_ARG
        + " option." + HELP_STR);
    }
    if (options.getTargetDir() != null) {
      throw new InvalidOptionsException("The " + TARGET_DIR_ARG
        + " option conflicts with the " + HCATALOG_TABLE_ARG
        + " option." + HELP_STR);
    }
    if (options.getWarehouseDir() != null) {
      throw new InvalidOptionsException("The " + WAREHOUSE_DIR_ARG
        + " option conflicts with the " + HCATALOG_TABLE_ARG
        + " option." + HELP_STR);
    }

    if (options.isAppendMode()) {
      throw new InvalidOptionsException("Append mode for imports is not "
        + " compatible with HCatalog. Please remove the parameter"
        + "--append-mode");
    }
    if (options.getExportDir() != null) {
      throw new InvalidOptionsException("The " + EXPORT_PATH_ARG
        + " option conflicts with the " + HCATALOG_TABLE_ARG
        + " option." + HELP_STR);
    }

    if (options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
      throw new InvalidOptionsException("HCatalog job is not compatible with "
        + " AVRO format option " + FMT_AVRODATAFILE_ARG
        + " option." + HELP_STR);

    }

    if (options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
      throw new InvalidOptionsException("HCatalog job  is not compatible with "
        + "SequenceFile format option " + FMT_SEQUENCEFILE_ARG
        + " option." + HELP_STR);
    }

    if (options.getFileLayout() == SqoopOptions.FileLayout.ParquetFile) {
      throw new InvalidOptionsException("HCatalog job  is not compatible with "
        + "SequenceFile format option " + FMT_PARQUETFILE_ARG
        + " option." + HELP_STR);
    }

    if (options.getHCatalogPartitionKeys() != null
        && options.getHCatalogPartitionValues() == null) {
      throw new InvalidOptionsException("Either both --hcatalog-partition-keys"
        + " and --hcatalog-partition-values should be provided or both of these"
        + " options should be omitted.");
    }

    if (options.getHCatalogPartitionKeys() != null) {
      if (options.getHivePartitionKey() != null) {
        LOG.warn("Both --hcatalog-partition-keys and --hive-partition-key"
            + "options are provided.  --hive-partition-key option will be"
            + "ignored");
      }

      String[] keys = options.getHCatalogPartitionKeys().split(",");
      String[] vals = options.getHCatalogPartitionValues().split(",");

      if (keys.length != vals.length) {
        throw new InvalidOptionsException("Number of static partition keys "
          + "provided dpes match the number of partition values");
      }

      for (int i = 0; i < keys.length; ++i) {
        String k = keys[i].trim();
        if (k.isEmpty()) {
          throw new InvalidOptionsException(
            "Invalid HCatalog static partition key at position " + i);
        }
      }
      for (int i = 0; i < vals.length; ++i) {
        String v = vals[i].trim();
        if (v.isEmpty()) {
          throw new InvalidOptionsException(
            "Invalid HCatalog static partition key at position " + v);
        }
      }
    } else {
      if (options.getHivePartitionKey() != null
          && options.getHivePartitionValue() == null) {
        throw new InvalidOptionsException("Either both --hive-partition-key and"
            + " --hive-partition-value options should be provided or both of "
            + "these options should be omitted");
      }
    }
    if (options.doCreateHCatalogTable() &&
            options.doDropAndCreateHCatalogTable()) {
      throw new InvalidOptionsException("Options --create-hcatalog-table" +
              " and --drop-and-create-hcatalog-table are mutually exclusive." +
              " Use any one of them");
    }
  }

  private boolean isSet(String option) {
    return org.apache.commons.lang.StringUtils.isNotBlank(option);
  }

  protected void validateHBaseOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if ((options.getHBaseColFamily() != null && options.getHBaseTable() == null)
        || (options.getHBaseColFamily() == null
        && options.getHBaseTable() != null)) {
      throw new InvalidOptionsException(
          "Both --hbase-table and --column-family must be set together."
          + HELP_STR);
    }

    if (options.isBulkLoadEnabled() && options.getHBaseTable() == null) {
      String validationMessage = String.format("Can't run import with %s "
          + "without %s",
          BaseSqoopTool.HBASE_BULK_LOAD_ENABLED_ARG,
          BaseSqoopTool.HBASE_TABLE_ARG);
      throw new InvalidOptionsException(validationMessage);
    }

    if (options.getHBaseTable() != null && options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      String validationMessage = String.format("Can't run HBase import with file layout: %s", options.getFileLayout());
      throw new InvalidOptionsException(validationMessage);
    }
  }

  /**
   * Given an array of extra arguments (usually populated via
   * this.extraArguments), determine the offset of the first '--'
   * argument in the list. Return 'extra.length' if there is none.
   */
  protected int getDashPosition(String [] extra) {
    int dashPos = extra.length;
    for (int i = 0; i < extra.length; i++) {
      if (extra[i].equals("--")) {
        dashPos = i;
        break;
      }
    }

    return dashPos;
  }
}

