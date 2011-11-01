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
import com.cloudera.sqoop.hive.HiveImport;

/**
 * Tool that creates a Hive table definition.
 */
public class CreateHiveTableTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(
      CreateHiveTableTool.class.getName());

  public CreateHiveTableTool() {
    super("create-hive-table");
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    if (!init(options)) {
      return 1;
    }

    try {
      HiveImport hiveImport = new HiveImport(options, manager,
          options.getConf(), false);
      hiveImport.importTable(options.getTableName(),
          options.getHiveTableName(), true);
    } catch (IOException ioe) {
      LOG.error("Encountered IOException running create table job: "
          + StringUtils.stringifyException(ioe));
      if (System.getProperty(Sqoop.SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(ioe);
      } else {
        return 1;
      }
    } finally {
      destroy(options);
    }

    return 0;
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {

    toolOptions.addUniqueOptions(getCommonOptions());

    RelatedOptions hiveOpts = getHiveOptions(false);
    hiveOpts.addOption(OptionBuilder.withArgName("table-name")
        .hasArg()
        .withDescription("The db table to read the definition from")
        .withLongOpt(TABLE_ARG)
        .create());
    toolOptions.addUniqueOptions(hiveOpts);

    toolOptions.addUniqueOptions(getOutputFormatOptions());
  }

  @Override
  /** {@inheritDoc} */
  public void printHelp(ToolOptions toolOptions) {
    super.printHelp(toolOptions);
    System.out.println("");
    System.out.println(
        "At minimum, you must specify --connect and --table");
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    if (in.hasOption(TABLE_ARG)) {
      out.setTableName(in.getOptionValue(TABLE_ARG));
    }

    out.setHiveImport(true);

    applyCommonOptions(in, out);
    applyHiveOptions(in, out);
    applyOutputFormatOptions(in, out);
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (hasUnrecognizedArgs(extraArguments)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    validateCommonOptions(options);
    validateOutputFormatOptions(options);
    validateHiveOptions(options);

    if (options.getTableName() == null) {
      throw new InvalidOptionsException(
          "--table is required for table definition importing." + HELP_STR);
    }
  }
}

