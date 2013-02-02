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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;

/**
 * Tool that lists available tables in a database.
 */
public class ListTablesTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(
      ListTablesTool.class.getName());

  public ListTablesTool() {
    super("list-tables");
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    if (!init(options)) {
      return 1;
    }

    try {
      String [] tables = manager.listTables();
      if (null == tables) {
        System.err.println("Could not retrieve tables list from server");
        LOG.error("manager.listTables() returned null");
        return 1;
      } else {
        for (String tbl : tables) {
          System.out.println(tbl);
        }
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
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    applyCommonOptions(in, out);
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {
    options.setExtraArgs(getSubcommandArgs(extraArguments));
    int dashPos = getDashPosition(extraArguments);
    if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
      throw new InvalidOptionsException(HELP_STR);
    }

    validateCommonOptions(options);
  }
}

