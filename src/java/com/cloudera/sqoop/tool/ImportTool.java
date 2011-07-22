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

package com.cloudera.sqoop.tool;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.hive.HiveImport;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;

/**
 * Tool that performs database imports to HDFS.
 */
public class ImportTool extends BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(ImportTool.class.getName());

  private CodeGenTool codeGenerator;

  // true if this is an all-tables import. Set by a subclass which
  // overrides the run() method of this tool (which can only do
  // a single table).
  private boolean allTables;

  public ImportTool() {
    this("import", false);
  }

  public ImportTool(String toolName, boolean allTables) {
    super(toolName);
    this.codeGenerator = new CodeGenTool();
    this.allTables = allTables;
  }

  @Override
  protected boolean init(SqoopOptions sqoopOpts) {
    boolean ret = super.init(sqoopOpts);
    codeGenerator.setManager(manager);
    return ret;
  }

  /**
   * @return a list of jar files generated as part of this import process
   */
  public List<String> getGeneratedJarFiles() {
    return this.codeGenerator.getGeneratedJarFiles();
  }

  protected void importTable(SqoopOptions options, String tableName,
      HiveImport hiveImport) throws IOException, ImportException {
    String jarFile = null;

    // Generate the ORM code for the tables.
    jarFile = codeGenerator.generateORM(options, tableName);

    // Do the actual import.
    ImportJobContext context = new ImportJobContext(tableName, jarFile,
        options);
    manager.importTable(context);

    // If the user wants this table to be in Hive, perform that post-load.
    if (options.doHiveImport()) {
      hiveImport.importTable(tableName, options.getHiveTableName(), false);
    }
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    HiveImport hiveImport = null;

    if (allTables) {
      // We got into this method, but we should be in a subclass.
      // (This method only handles a single table)
      // This should not be reached, but for sanity's sake, test here.
      LOG.error("ImportTool.run() can only handle a single table.");
      return 1;
    }

    if (!init(options)) {
      return 1;
    }

    codeGenerator.setManager(manager);

    try {
      if (options.doHiveImport()) {
        hiveImport = new HiveImport(options, manager, options.getConf(), false);
      }

      // Import a single table the user specified.
      importTable(options, options.getTableName(), hiveImport);
    } catch (IOException ioe) {
      LOG.error("Encountered IOException running import job: "
          + ioe.toString());
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ioe);
      } else {
        return 1;
      }
    } catch (ImportException ie) {
      LOG.error("Error during import: " + ie.toString());
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ie);
      } else {
        return 1;
      }
    } finally {
      destroy(options);
    }

    return 0;
  }

  /**
   * Construct the set of options that control imports, either of one
   * table or a batch of tables.
   * @return the RelatedOptions that can be used to parse the import
   * arguments.
   */
  protected RelatedOptions getImportOptions() {
    // Imports
    RelatedOptions importOpts = new RelatedOptions("Import control arguments");

    importOpts.addOption(OptionBuilder
        .withDescription("Use direct import fast path")
        .withLongOpt(DIRECT_ARG)
        .create());

    if (!allTables) {
      importOpts.addOption(OptionBuilder.withArgName("table-name")
          .hasArg().withDescription("Table to read")
          .withLongOpt(TABLE_ARG)
          .create());
      importOpts.addOption(OptionBuilder.withArgName("col,col,col...")
          .hasArg().withDescription("Columns to import from table")
          .withLongOpt(COLUMNS_ARG)
          .create());
      importOpts.addOption(OptionBuilder.withArgName("column-name")
          .hasArg()
          .withDescription("Column of the table used to split work units")
          .withLongOpt(SPLIT_BY_ARG)
          .create());
      importOpts.addOption(OptionBuilder.withArgName("where clause")
          .hasArg().withDescription("WHERE clause to use during import")
          .withLongOpt(WHERE_ARG)
          .create());
    }

    importOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg().withDescription("HDFS parent for table destination")
        .withLongOpt(WAREHOUSE_DIR_ARG)
        .create());
    importOpts.addOption(OptionBuilder
        .withDescription("Imports data to SequenceFiles")
        .withLongOpt(FMT_SEQUENCEFILE_ARG)
        .create());
    importOpts.addOption(OptionBuilder
        .withDescription("Imports data as plain text (default)")
        .withLongOpt(FMT_TEXTFILE_ARG)
        .create());
    importOpts.addOption(OptionBuilder.withArgName("n")
        .hasArg().withDescription("Use 'n' map tasks to import in parallel")
        .withLongOpt(NUM_MAPPERS_ARG)
        .create(NUM_MAPPERS_SHORT_ARG));
    importOpts.addOption(OptionBuilder
        .withDescription("Enable compression")
        .withLongOpt(COMPRESS_ARG)
        .create(COMPRESS_SHORT_ARG));
    importOpts.addOption(OptionBuilder.withArgName("n")
        .hasArg()
        .withDescription("Split the input stream every 'n' bytes "
        + "when importing in direct mode")
        .withLongOpt(DIRECT_SPLIT_SIZE_ARG)
        .create());
    importOpts.addOption(OptionBuilder.withArgName("n")
        .hasArg()
        .withDescription("Set the maximum size for an inline LOB")
        .withLongOpt(INLINE_LOB_LIMIT_ARG)
        .create());

    return importOpts;
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {

    toolOptions.addUniqueOptions(getCommonOptions());
    toolOptions.addUniqueOptions(getImportOptions());
    toolOptions.addUniqueOptions(getOutputFormatOptions());
    toolOptions.addUniqueOptions(getInputFormatOptions());
    toolOptions.addUniqueOptions(getHiveOptions(true));

    // get common codegen opts.
    RelatedOptions codeGenOpts = getCodeGenOpts(allTables);

    // add import-specific codegen opts:
    codeGenOpts.addOption(OptionBuilder.withArgName("file")
        .hasArg()
        .withDescription("Disable code generation; use specified jar")
        .withLongOpt(JAR_FILE_NAME_ARG)
        .create());

    toolOptions.addUniqueOptions(codeGenOpts);
  }

  @Override
  /** {@inheritDoc} */
  public void printHelp(ToolOptions toolOptions) {
    super.printHelp(toolOptions);
    System.out.println("");
    if (allTables) {
      System.out.println("At minimum, you must specify --connect");
    } else {
      System.out.println(
          "At minimum, you must specify --connect and --table");
    } 

    System.out.println(
        "Arguments to mysqldump and other subprograms may be supplied");
    System.out.println(
        "after a '--' on the command line.");
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    try {
      applyCommonOptions(in, out);

      if (in.hasOption(DIRECT_ARG)) {
        out.setDirectMode(true);
      }

      if (!allTables) {
        if (in.hasOption(TABLE_ARG)) {
          out.setTableName(in.getOptionValue(TABLE_ARG));
        }

        if (in.hasOption(COLUMNS_ARG)) {
          out.setColumns(in.getOptionValue(COLUMNS_ARG).split(","));
        }

        if (in.hasOption(SPLIT_BY_ARG)) {
          out.setSplitByCol(in.getOptionValue(SPLIT_BY_ARG));
        }

        if (in.hasOption(WHERE_ARG)) {
          out.setWhereClause(in.getOptionValue(WHERE_ARG));
        }
      }

      if (in.hasOption(WAREHOUSE_DIR_ARG)) {
        out.setWarehouseDir(in.getOptionValue(WAREHOUSE_DIR_ARG));
      }

      if (in.hasOption(FMT_SEQUENCEFILE_ARG)) {
        out.setFileLayout(SqoopOptions.FileLayout.SequenceFile);
      }

      if (in.hasOption(FMT_TEXTFILE_ARG)) {
        out.setFileLayout(SqoopOptions.FileLayout.TextFile);
      }

      if (in.hasOption(NUM_MAPPERS_ARG)) {
        out.setNumMappers(Integer.parseInt(in.getOptionValue(NUM_MAPPERS_ARG)));
      }

      if (in.hasOption(COMPRESS_ARG)) {
        out.setUseCompression(true);
      }

      if (in.hasOption(DIRECT_SPLIT_SIZE_ARG)) {
        out.setDirectSplitSize(Long.parseLong(in.getOptionValue(
            DIRECT_SPLIT_SIZE_ARG)));
      }

      if (in.hasOption(INLINE_LOB_LIMIT_ARG)) {
        out.setInlineLobLimit(Long.parseLong(in.getOptionValue(
            INLINE_LOB_LIMIT_ARG)));
      }

      if (in.hasOption(JAR_FILE_NAME_ARG)) {
        out.setExistingJarName(in.getOptionValue(JAR_FILE_NAME_ARG));
      }

      applyHiveOptions(in, out);
      applyOutputFormatOptions(in, out);
      applyInputFormatOptions(in, out);
      applyCodeGenOptions(in, out, allTables);
    } catch (NumberFormatException nfe) {
      throw new InvalidOptionsException("Error: expected numeric argument.\n"
          + "Try --help for usage.");
    }
  }

  /**
   * Validate import-specific arguments.
   * @param options the configured SqoopOptions to check
   */
  protected void validateImportOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (!allTables && options.getTableName() == null) {
      throw new InvalidOptionsException(
          "--table is required for import. (Or use sqoop import-all-tables.)"
          + HELP_STR);
    } else if (options.getExistingJarName() != null
        && options.getClassName() == null) {
      throw new InvalidOptionsException("Jar specified with --jar-file, but no "
          + "class specified with --class-name." + HELP_STR);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    // If extraArguments is full, check for '--' followed by args for
    // mysqldump or other commands we rely on.
    options.setExtraArgs(getSubcommandArgs(extraArguments));
    int dashPos = extraArguments.length;
    for (int i = 0; i < extraArguments.length; i++) {
      if (extraArguments[i].equals("--")) {
        dashPos = i;
        break;
      }
    }

    if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    validateImportOptions(options);
    validateCommonOptions(options);
    validateCodeGenOptions(options);
    validateOutputFormatOptions(options);
  }
}

