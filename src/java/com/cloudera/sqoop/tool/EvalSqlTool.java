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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.cli.ToolOptions;

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

    PreparedStatement stmt = null;
    try {
      if (options.isSqlQueryUpdate()) {
        // Run the SQL statement and commit the tx.
        Connection c = manager.getConnection();
        stmt = c.prepareStatement(options.getSqlQuery());
        stmt.executeUpdate();
        c.commit();
      } else {
        // Run the SQL statement and print the results.
        manager.execAndPrint(options.getSqlQuery());
      }
    } catch (SQLException sqlE) {
      LOG.warn("SQL exception executing statement: "
          + StringUtils.stringifyException(sqlE));
      return 1;
    } finally {
      if (null != stmt) {
        try {
          stmt.close();
        } catch (SQLException sqlE) {
          LOG.warn("SQL exception closing statement: "
              + StringUtils.stringifyException(sqlE));
          return 1;
        }
      }
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
        .withLongOpt(SQL_QUERY_ARG)
        .create(SQL_QUERY_SHORT_ARG));
    evalOpts.addOption(OptionBuilder
        .withDescription("Execute an update operation")
        .withLongOpt(SQL_QUERY_UPDATE_ARG)
        .create());

    toolOptions.addUniqueOptions(evalOpts);
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    applyCommonOptions(in, out);
    if (in.hasOption(SQL_QUERY_ARG)) {
      out.setSqlQuery(in.getOptionValue(SQL_QUERY_ARG));
    }

    if (in.hasOption(SQL_QUERY_UPDATE_ARG)) {
      out.setSqlQueryIsUpdate(true);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (hasUnrecognizedArgs(extraArguments)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    String sqlCmd = options.getSqlQuery();
    if (null == sqlCmd || sqlCmd.length() == 0) {
      throw new InvalidOptionsException(
          "This command requires the " + SQL_QUERY_ARG + " argument."
          + HELP_STR);
    }

    validateCommonOptions(options);
  }
}

