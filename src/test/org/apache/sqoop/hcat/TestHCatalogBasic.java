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

package org.apache.sqoop.hcat;

import org.junit.Before;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.ImportTool;

import org.junit.Test;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Test basic HCatalog related features.
 */
public class TestHCatalogBasic {

  private static ImportTool importTool;
  private static ExportTool exportTool;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    importTool = new ImportTool();
    exportTool = new ExportTool();
  }
  private SqoopOptions parseImportArgs(String[] argv) throws Exception {
    SqoopOptions opts = importTool.parseArguments(argv, null, null, false);
    return opts;
  }

  private SqoopOptions parseExportArgs(String[] argv) throws Exception {
    SqoopOptions opts = exportTool.parseArguments(argv, null, null, false);
    return opts;
  }

  @Test
  public void testHCatalogHomeWithImport() throws Exception {
    String[] args = {
      "--hcatalog-home",
      "/usr/lib/hcatalog",
    };

    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatalogHomeWithExport() throws Exception {
    String[] args = {
      "--hcatalog-home",
      "/usr/lib/hcatalog",
    };

    SqoopOptions opts = parseExportArgs(args);
  }

  @Test
  public void testHCatalogImport() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
    };

    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatalogExport() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
    };

    SqoopOptions opts = parseExportArgs(args);
  }

  @Test
  public void testHCatImportWithTargetDir() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--target-dir",
      "/target/dir",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --target-dir");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithWarehouseDir() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--warehouse-dir",
      "/target/dir",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --warehouse-dir");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithHiveImport() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--hive-import",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --hive-import");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatExportWithExportDir() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--export-dir",
      "/export/dir",
    };
    SqoopOptions opts = parseExportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog export " +
        "with --export-dir");
    exportTool.validateOptions(opts);
  }

  @Test
  public void testHCatExportWithParquetFile() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--as-parquetfile",
    };
    SqoopOptions opts = parseExportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog export " +
        "with --as-parquetfile");
    exportTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithSequenceFile() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--as-sequencefile",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --as-sequencefile");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithParquetFile() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
      "--create-hcatalog-table",
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--as-parquetfile",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --as-parquetfile");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithAvroFile() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--as-avrodatafile",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --as-avrodatafile");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithCreateTable() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
      "--create-hcatalog-table",
    };
    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatImportWithDropAndCreateTable() throws Exception {
    String[] args = {
            "--connect",
            "jdbc:db:url",
            "--table",
            "dbtable",
            "--hcatalog-table",
            "table",
            "--drop-and-create-hcatalog-table",
    };
    SqoopOptions opts = parseImportArgs(args);
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithCreateTableAndDropAndCreateTable()
    throws Exception {
    String[] args = {
            "--connect",
            "jdbc:db:url",
            "--table",
            "dbtable",
            "--hcatalog-table",
            "table",
            "--create-hcatalog-table",
            "--drop-and-create-hcatalog-table",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with --drop-and-create-hcatalog-table");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithStorageStanza() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
      "--hcatalog-storage-stanza",
      "stored as textfile",
    };
    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatImportWithDatabase() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
      "--hcatalog-database",
      "default",
    };
    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatImportWithPartKeys() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "table",
      "--hcatalog-partition-keys",
      "k1,k2",
      "--hcatalog-partition-values",
      "v1,v2",
    };
    SqoopOptions opts = parseImportArgs(args);
  }

  @Test
  public void testHCatImportWithOnlyHCatKeys() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--hcatalog-partition-keys",
      "k1,k2",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with only HCatalog keys");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithMismatchedKeysAndVals() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--hcatalog-partition-keys",
      "k1,k2",
      "--hcatalog-partition-values",
      "v1",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with mismatched keys and values");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithEmptyKeysAndVals() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--hcatalog-partition-keys",
      "k1,",
      "--hcatalog-partition-values",
      ",v1",
    };
    SqoopOptions opts = parseImportArgs(args);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.reportMissingExceptionWithMessage("Expected InvalidOptionsException during HCatalog import " +
        "with empty keys and values");
    importTool.validateOptions(opts);
  }

  @Test
  public void testHCatImportWithBothHCatAndHivePartOptions() throws Exception {
    String[] args = {
      "--connect",
      "jdbc:db:url",
      "--table",
      "dbtable",
      "--hcatalog-table",
      "table",
      "--hcatalog-partition-keys",
      "k1,k2",
      "--hcatalog-partition-values",
      "v1,v2",
      "--hive-partition-key",
      "k1",
      "--hive-partition-value",
      "v1",
    };
    SqoopOptions opts = parseImportArgs(args);
    importTool.validateOptions(opts);
  }

}
