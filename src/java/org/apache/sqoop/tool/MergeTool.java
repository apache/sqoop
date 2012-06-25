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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.mapreduce.MergeJob;
import org.apache.sqoop.util.LoggingUtils;

/**
 * Tool that merges a more recent dataset on top of an older one.
 */
public class MergeTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(MergeTool.class.getName());

  public MergeTool() {
    this("merge");
  }

  public MergeTool(String toolName) {
    super(toolName);
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    try {
      // Configure and execute a MapReduce job to merge these datasets.
      MergeJob mergeJob = new MergeJob(options);
      if (!mergeJob.runMergeJob()) {
        LOG.error("MapReduce job failed!");
        return 1;
      }
    } catch (IOException ioe) {
      LOG.error("Encountered IOException running import job: "
          + StringUtils.stringifyException(ioe));
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ioe);
      } else {
        return 1;
      }
    }

    return 0;
  }

  /**
   * Construct the set of options that control imports, either of one
   * table or a batch of tables.
   * @return the RelatedOptions that can be used to parse the import
   * arguments.
   */
  protected RelatedOptions getMergeOptions() {
    // Imports
    RelatedOptions mergeOpts = new RelatedOptions("Merge arguments");

    mergeOpts.addOption(OptionBuilder.withArgName("file")
        .hasArg().withDescription("Load class from specified jar file")
        .withLongOpt(JAR_FILE_NAME_ARG)
        .create());

    mergeOpts.addOption(OptionBuilder.withArgName("name")
        .hasArg().withDescription("Specify record class name to load")
        .withLongOpt(CLASS_NAME_ARG)
        .create());

    mergeOpts.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("Path to the more recent data set")
        .withLongOpt(NEW_DATASET_ARG)
        .create());

    mergeOpts.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("Path to the older data set")
        .withLongOpt(OLD_DATASET_ARG)
        .create());

    mergeOpts.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("Destination path for merged results")
        .withLongOpt(TARGET_DIR_ARG)
        .create());

    mergeOpts.addOption(OptionBuilder.withArgName("column")
        .hasArg().withDescription("Key column to use to join results")
        .withLongOpt(MERGE_KEY_ARG)
        .create());

    // Since the "common" options aren't used in the merge tool,
    // add these settings here.
    mergeOpts.addOption(OptionBuilder
        .withDescription("Print more information while working")
        .withLongOpt(VERBOSE_ARG)
        .create());
    mergeOpts.addOption(OptionBuilder
        .withDescription("Print usage instructions")
        .withLongOpt(HELP_ARG)
        .create());

    return mergeOpts;
  }


  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {
    toolOptions.addUniqueOptions(getMergeOptions());
  }


  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    if (in.hasOption(VERBOSE_ARG)) {
      LoggingUtils.setDebugLevel();
      LOG.debug("Enabled debug logging.");
    }

    if (in.hasOption(HELP_ARG)) {
      ToolOptions toolOpts = new ToolOptions();
      configureOptions(toolOpts);
      printHelp(toolOpts);
      throw new InvalidOptionsException("");
    }

    if (in.hasOption(JAR_FILE_NAME_ARG)) {
      out.setExistingJarName(in.getOptionValue(JAR_FILE_NAME_ARG));
    }

    if (in.hasOption(CLASS_NAME_ARG)) {
      out.setClassName(in.getOptionValue(CLASS_NAME_ARG));
    }

    if (in.hasOption(NEW_DATASET_ARG)) {
      out.setMergeNewPath(in.getOptionValue(NEW_DATASET_ARG));
    }

    if (in.hasOption(OLD_DATASET_ARG)) {
      out.setMergeOldPath(in.getOptionValue(OLD_DATASET_ARG));
    }

    if (in.hasOption(TARGET_DIR_ARG)) {
      out.setTargetDir(in.getOptionValue(TARGET_DIR_ARG));
    }

    if (in.hasOption(MERGE_KEY_ARG)) {
      out.setMergeKeyCol(in.getOptionValue(MERGE_KEY_ARG));
    }
  }

  /**
   * Validate merge-specific arguments.
   * @param options the configured SqoopOptions to check
   */
  protected void validateMergeOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (options.getMergeNewPath() == null) {
      throw new InvalidOptionsException("Must set the new dataset path with --"
          + NEW_DATASET_ARG + "." + HELP_STR);
    }

    if (options.getMergeOldPath() == null) {
      throw new InvalidOptionsException("Must set the old dataset path with --"
          + OLD_DATASET_ARG + "." + HELP_STR);
    }

    if (options.getMergeKeyCol() == null) {
      throw new InvalidOptionsException("Must set the merge key column with --"
          + MERGE_KEY_ARG + "." + HELP_STR);
    }

    if (options.getTargetDir() == null) {
      throw new InvalidOptionsException("Must set the target directory with --"
          + TARGET_DIR_ARG + "." + HELP_STR);
    }

    if (options.getClassName() == null) {
      throw new InvalidOptionsException("Must set the SqoopRecord class "
          + "implementation to use with --" + CLASS_NAME_ARG + "."
          + HELP_STR);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    // If extraArguments is full, check for '--' followed by args for
    // mysqldump or other commands we rely on.
    options.setExtraArgs(getSubcommandArgs(extraArguments));
    int dashPos = getDashPosition(extraArguments);

    if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    validateMergeOptions(options);
  }
}

