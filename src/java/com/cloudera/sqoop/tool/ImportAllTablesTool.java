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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hive.HiveImport;
import com.cloudera.sqoop.util.ImportException;

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
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    HiveImport hiveImport = null;

    if (!init(options)) {
      return 1;
    }

    try {
      if (options.doHiveImport()) {
        hiveImport = new HiveImport(options, manager, options.getConf(), false);
      }

      String [] tables = manager.listTables();
      if (null == tables) {
        System.err.println("Could not retrieve tables list from server");
        LOG.error("manager.listTables() returned null");
        return 1;
      } else {
        for (String tableName : tables) {
          importTable(options, tableName, hiveImport);
        }
      }
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

}

