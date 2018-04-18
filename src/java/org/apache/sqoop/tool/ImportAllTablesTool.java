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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.util.ImportException;

/**
 * Tool that performs database imports of all tables in a database to HDFS.
 */
public class ImportAllTablesTool extends ImportTool {

  public static final Log LOG = LogFactory.getLog(
      ImportAllTablesTool.class.getName());

  public ImportAllTablesTool() {
    super("import-all-tables", true);
  }

  @Override
  @SuppressWarnings("static-access")
  /** {@inheritDoc} */
  protected RelatedOptions getImportOptions() {
    // Imports
    RelatedOptions importOpts = super.getImportOptions();

    importOpts.addOption(OptionBuilder.withArgName("tables")
        .hasArg().withDescription("Tables to exclude when importing all tables")
        .withLongOpt(ALL_TABLE_EXCLUDES_ARG)
        .create());

    return importOpts;
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    super.applyOptions(in, out);

    if (in.hasOption(ALL_TABLE_EXCLUDES_ARG)) {
      out.setAllTablesExclude(in.getOptionValue(ALL_TABLE_EXCLUDES_ARG));
    }
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    Set<String> excludes = new HashSet<String>();

    if (!init(options)) {
      return 1;
    }

    try {

      if (options.getAllTablesExclude() != null) {
        excludes.addAll(Arrays.asList(options.getAllTablesExclude().split(",")));
      }

      String [] tables = manager.listTables();
      if (null == tables) {
        System.err.println("Could not retrieve tables list from server");
        LOG.error("manager.listTables() returned null");
        return 1;
      } else {
        for (String tableName : tables) {
          if (excludes.contains(tableName)) {
            System.out.println("Skipping table: " + tableName);
          } else {
            SqoopOptions clonedOptions = (SqoopOptions) options.clone();
            clonedOptions.setTableName(tableName);
            importTable(clonedOptions);
          }
        }
      }
    } catch (IOException ioe) {
      LOG.error("Encountered IOException running import job: "
          + ioe.toString());
      rethrowIfRequired(options, ioe);
      return 1;
    } catch (ImportException ie) {
      LOG.error("Error during import: " + ie.toString());
      rethrowIfRequired(options, ie);
      return 1;
    } finally {
      destroy(options);
    }

    return 0;
  }

}

