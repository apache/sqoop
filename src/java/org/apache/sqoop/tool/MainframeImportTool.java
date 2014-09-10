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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;

/**
 * Tool that performs mainframe dataset imports to HDFS.
 */
public class MainframeImportTool extends ImportTool {
  private static final Log LOG
      = LogFactory.getLog(MainframeImportTool.class.getName());
  public static final String DS_ARG = "dataset";

  public MainframeImportTool() {
    super("import-mainframe", false);
  }

  @Override
  @SuppressWarnings("static-access")
  protected RelatedOptions getImportOptions() {
    // Imports
    RelatedOptions importOpts
        = new RelatedOptions("Import mainframe control arguments");
    importOpts.addOption(OptionBuilder.withArgName("Dataset name")
        .hasArg().withDescription("Datasets to import")
        .withLongOpt(DS_ARG)
        .create());
    importOpts.addOption(OptionBuilder
        .withDescription("Imports data in delete mode")
        .withLongOpt(DELETE_ARG)
        .create());
    importOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg().withDescription("HDFS plain file destination")
        .withLongOpt(TARGET_DIR_ARG)
        .create());

    addValidationOpts(importOpts);

    importOpts.addOption(OptionBuilder.withArgName("dir")
        .hasArg().withDescription("HDFS parent for file destination")
        .withLongOpt(WAREHOUSE_DIR_ARG)
        .create());
    importOpts.addOption(OptionBuilder
        .withDescription("Imports data as plain text (default)")
        .withLongOpt(FMT_TEXTFILE_ARG)
        .create());
    importOpts.addOption(OptionBuilder.withArgName("n")
        .hasArg().withDescription("Use 'n' map tasks to import in parallel")
        .withLongOpt(NUM_MAPPERS_ARG)
        .create(NUM_MAPPERS_SHORT_ARG));
    importOpts.addOption(OptionBuilder.withArgName("name")
        .hasArg().withDescription("Set name for generated mapreduce job")
        .withLongOpt(MAPREDUCE_JOB_NAME)
        .create());
    importOpts.addOption(OptionBuilder
        .withDescription("Enable compression")
        .withLongOpt(COMPRESS_ARG)
        .create(COMPRESS_SHORT_ARG));
    importOpts.addOption(OptionBuilder.withArgName("codec")
        .hasArg()
        .withDescription("Compression codec to use for import")
        .withLongOpt(COMPRESSION_CODEC_ARG)
        .create());

    return importOpts;
  }

  @Override
  public void configureOptions(ToolOptions toolOptions) {
    toolOptions.addUniqueOptions(getCommonOptions());
    toolOptions.addUniqueOptions(getImportOptions());
    toolOptions.addUniqueOptions(getOutputFormatOptions());
    toolOptions.addUniqueOptions(getInputFormatOptions());
    toolOptions.addUniqueOptions(getHiveOptions(true));
    toolOptions.addUniqueOptions(getHBaseOptions());
    toolOptions.addUniqueOptions(getHCatalogOptions());
    toolOptions.addUniqueOptions(getHCatImportOnlyOptions());
    toolOptions.addUniqueOptions(getAccumuloOptions());

    // get common codegen opts.
    RelatedOptions codeGenOpts = getCodeGenOpts(false);

    // add import-specific codegen opts:
    codeGenOpts.addOption(OptionBuilder.withArgName("file")
        .hasArg()
        .withDescription("Disable code generation; use specified jar")
        .withLongOpt(JAR_FILE_NAME_ARG)
        .create());

    toolOptions.addUniqueOptions(codeGenOpts);
  }

  @Override
  public void printHelp(ToolOptions toolOptions) {
    System.out.println("usage: sqoop " + getToolName()
        + " [GENERIC-ARGS] [TOOL-ARGS]\n");

    toolOptions.printHelp();

    System.out.println("\nGeneric Hadoop command-line arguments:");
    System.out.println("(must preceed any tool-specific arguments)");
    ToolRunner.printGenericCommandUsage(System.out);
    System.out.println(
        "\nAt minimum, you must specify --connect and --" + DS_ARG);
  }

  @Override
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    super.applyOptions(in, out);

    if (!in.hasOption(CONN_MANAGER_CLASS_NAME)) {
       // set default ConnManager
      out.setConnManagerClassName("org.apache.sqoop.manager.MainframeManager");
    }
    if (in.hasOption(DS_ARG)) {
      out.setMainframeInputDatasetName(in.getOptionValue(DS_ARG));
    }
  }

  @Override
  protected void validateImportOptions(SqoopOptions options)
      throws InvalidOptionsException {
    if (options.getMainframeInputDatasetName() == null) {
      throw new InvalidOptionsException(
          "--" + DS_ARG + " is required for mainframe import. " + HELP_STR);
    }
    super.validateImportOptions(options);
  }
}
