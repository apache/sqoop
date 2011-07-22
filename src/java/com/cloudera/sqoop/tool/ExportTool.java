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
import java.util.ArrayList;
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
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.ExportException;

/**
 * Tool that performs HDFS exports to databases.
 */
public class ExportTool extends BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(ExportTool.class.getName());

  private CodeGenTool codeGenerator;

  public ExportTool() {
    super("export");
    this.codeGenerator = new CodeGenTool();
  }

  /**
   * @return a list of jar files generated as part of this im/export process
   */
  public List<String> getGeneratedJarFiles() {
    return codeGenerator.getGeneratedJarFiles();
  }

  private void exportTable(SqoopOptions options, String tableName)
      throws ExportException, IOException {
    String jarFile = null;

    // Generate the ORM code for the tables.
    jarFile = codeGenerator.generateORM(options, tableName);

    ExportJobContext context = new ExportJobContext(tableName, jarFile,
        options);
    if (options.getUpdateKeyCol() != null) {
      // UPDATE-based export.
      manager.updateTable(context);
    } else {
      // INSERT-based export.
      manager.exportTable(context);
    }
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {

    if (!init(options)) {
      return 1;
    }

    codeGenerator.setManager(manager);

    String updateKeyCol = options.getUpdateKeyCol();
    if (updateKeyCol != null) {
      // We're in update mode. We need to explicitly set the database output
      // column ordering in the codeGenerator.  The UpdateKeyCol must come
      // last, because the UPDATE-based OutputFormat will generate the SET
      // clause followed by the WHERE clause, and the SqoopRecord needs to
      // serialize to this layout.
      String [] allColNames = manager.getColumnNames(options.getTableName());
      List<String> dbOutCols = new ArrayList<String>();
      String upperCaseKeyCol = updateKeyCol.toUpperCase();
      for (String col : allColNames) {
        if (!upperCaseKeyCol.equals(col.toUpperCase())) {
          dbOutCols.add(col); // add non-key columns to the output order list.
        }
      }

      // Then add the update key column last.
      dbOutCols.add(updateKeyCol);
      options.setDbOutputColumns(dbOutCols.toArray(
          new String[dbOutCols.size()]));
    }

    try {
      exportTable(options, options.getTableName());
    } catch (IOException ioe) {
      LOG.error("Encountered IOException running export job: "
          + ioe.toString());
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ioe);
      } else {
        return 1;
      }
    } catch (ExportException ee) {
      LOG.error("Error during export: " + ee.toString());
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ee);
      } else {
        return 1;
      }
    } finally {
      destroy(options);
    }

    return 0;
  }

  /**
   * Construct the set of options that control exports.
   * @return the RelatedOptions that can be used to parse the export
   * arguments.
   */
  protected RelatedOptions getExportOptions() {
    RelatedOptions exportOpts = new RelatedOptions("Export control arguments");

    exportOpts.addOption(OptionBuilder
        .withDescription("Use direct export fast path")
        .withLongOpt(DIRECT_ARG)
        .create());
    exportOpts.addOption(OptionBuilder.withArgName("table-name")
        .hasArg().withDescription("Table to populate")
        .withLongOpt(TABLE_ARG)
        .create());
    exportOpts.addOption(OptionBuilder.withArgName("n")
        .hasArg().withDescription("Use 'n' map tasks to export in parallel")
        .withLongOpt(NUM_MAPPERS_ARG)
        .create(NUM_MAPPERS_SHORT_ARG));
    exportOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg()
        .withDescription("HDFS source path for the export")
        .withLongOpt(EXPORT_PATH_ARG)
        .create());
    exportOpts.addOption(OptionBuilder.withArgName("key")
        .hasArg()
        .withDescription("Update records by specified key column")
        .withLongOpt(UPDATE_KEY_ARG)
        .create());

    return exportOpts;
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {

    toolOptions.addUniqueOptions(getCommonOptions());
    toolOptions.addUniqueOptions(getExportOptions());

    // Input parsing delimiters
    toolOptions.addUniqueOptions(getInputFormatOptions());

    // Used when sending data to a direct-mode export.
    toolOptions.addUniqueOptions(getOutputFormatOptions());

    // get common codegen opts.
    RelatedOptions codeGenOpts = getCodeGenOpts(false);

    // add export-specific codegen opts:
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
    System.out.println(
        "At minimum, you must specify --connect, --export-dir, and --table");
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

      if (in.hasOption(TABLE_ARG)) {
        out.setTableName(in.getOptionValue(TABLE_ARG));
      }

      if (in.hasOption(NUM_MAPPERS_ARG)) {
        out.setNumMappers(Integer.parseInt(in.getOptionValue(NUM_MAPPERS_ARG)));
      }

      if (in.hasOption(EXPORT_PATH_ARG)) {
        out.setExportDir(in.getOptionValue(EXPORT_PATH_ARG));
      }

      if (in.hasOption(JAR_FILE_NAME_ARG)) {
        out.setExistingJarName(in.getOptionValue(JAR_FILE_NAME_ARG));
      }

      if (in.hasOption(UPDATE_KEY_ARG)) {
        out.setUpdateKeyCol(in.getOptionValue(UPDATE_KEY_ARG));
      }

      applyInputFormatOptions(in, out);
      applyOutputFormatOptions(in, out);
      applyOutputFormatOptions(in, out);
      applyCodeGenOptions(in, out, false);
    } catch (NumberFormatException nfe) {
      throw new InvalidOptionsException("Error: expected numeric argument.\n"
          + "Try --help for usage.");
    }
  }

  /**
   * Validate export-specific arguments.
   * @param options the configured SqoopOptions to check
   */
  protected void validateExportOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.getTableName() == null) {
      throw new InvalidOptionsException("Export requires a --table argument."
          + HELP_STR);
    } else if (options.getExportDir() == null) {
      throw new InvalidOptionsException(
          "Export requires an --export-dir argument."
          + HELP_STR);
    } else if (options.getExistingJarName() != null
        && options.getClassName() == null) {
      throw new InvalidOptionsException("Jar specified with --jar-file, but no "
          + "class specified with --class-name." + HELP_STR);
    } else if (options.getExistingJarName() != null
        && options.getUpdateKeyCol() != null) {
      // We need to regenerate the class with the output column order set
      // correctly for the update-based export. So we can't use a premade
      // class.
      throw new InvalidOptionsException("Jar cannot be specified with "
          + "--jar-file when export is running in update mode.");
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

    validateExportOptions(options);
    validateOutputFormatOptions(options);
    validateCommonOptions(options);
    validateCodeGenOptions(options);
  }
}

