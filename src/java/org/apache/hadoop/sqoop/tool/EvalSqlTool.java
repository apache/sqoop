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

package org.apache.hadoop.sqoop.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.SqoopOptions.InvalidOptionsException;
import org.apache.hadoop.sqoop.cli.RelatedOptions;
import org.apache.hadoop.sqoop.cli.ToolOptions;

/**
 * Tool that evaluates a SQL statement and displays the results.
 */
public class EvalSqlTool extends BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(EvalSqlTool.class.getName());

  public EvalSqlTool() {
    super("eval");
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    if (!init(options)) {
      return 1;
    }

    try {
      // just run a SQL statement for debugging purposes.
      manager.execAndPrint(options.getDebugSqlCmd());
    } finally {
      destroy(options);
    }

    return 0;
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {
    toolOptions.addUniqueOptions(getCommonOptions());

    RelatedOptions evalOpts = new RelatedOptions("SQL evaluation arguments");
    evalOpts.addOption(OptionBuilder.withArgName("statement")
        .hasArg()
        .withDescription("Execute 'statement' in SQL and exit")
        .withLongOpt(DEBUG_SQL_ARG)
        .create(DEBUG_SQL_SHORT_ARG));

    toolOptions.addUniqueOptions(evalOpts);
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    applyCommonOptions(in, out);
    if (in.hasOption(DEBUG_SQL_ARG)) {
      out.setDebugSqlCmd(in.getOptionValue(DEBUG_SQL_ARG));
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (hasUnrecognizedArgs(extraArguments)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    String sqlCmd = options.getDebugSqlCmd();
    if (null == sqlCmd || sqlCmd.length() == 0) {
      throw new InvalidOptionsException(
          "This command requires the " + DEBUG_SQL_ARG + " argument."
          + HELP_STR);
    }

    validateCommonOptions(options);
  }
}

