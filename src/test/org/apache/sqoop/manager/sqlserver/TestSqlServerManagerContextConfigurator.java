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

package org.apache.sqoop.manager.sqlserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ExportJobContext;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.manager.SqlServerManagerContextConfigurator;
import org.apache.sqoop.mapreduce.SQLServerResilientExportOutputFormat;
import org.apache.sqoop.mapreduce.SQLServerResilientUpdateOutputFormat;
import org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.sqoop.mapreduce.db.SQLServerDBInputFormat;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerExportBatchOutputFormat;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test methods in the configuration utilities.
 */
@Category(UnitTest.class)
public class TestSqlServerManagerContextConfigurator {

  public static final Log LOG = LogFactory.getLog(TestSqlServerManagerContextConfigurator.class.getName());

  private final SqlServerManagerContextConfigurator formatConfigurator = new SqlServerManagerContextConfigurator();

  private SqoopOptions options;

  @Test
  public void testResilientImportContextConfiguration() {
    String[] extraArgs = {"--resilient"};
    options.setExtraArgs(extraArgs);

    ImportJobContext context = new ImportJobContext("TABLE_NAME", "example.jar", options, null);
    formatConfigurator.configureContextForImport(context, "id");
    Class<? extends InputFormat> inputFormat = context.getInputFormat();
    assertThat(inputFormat).isSameAs(SQLServerDBInputFormat.class);
  }

  @Test
  public void testNonResilientImportContextConfiguration() {
    String[] extraArgs = {"--non-resilient"};
    options.setExtraArgs(extraArgs);

    ImportJobContext context = new ImportJobContext("TABLE_NAME", "example.jar", options, null);
    formatConfigurator.configureContextForImport(context, "id");
    Class<? extends InputFormat> inputFormat = context.getInputFormat();
    assertThat(inputFormat).isSameAs(DataDrivenDBInputFormat.class);
  }

  @Test
  public void testResilientExportContextConfiguration() {
    String[] extraArgs = {"--resilient"};
    options.setExtraArgs(extraArgs);

    ExportJobContext context = new ExportJobContext("TABLE_NAME", "example.jar", options);
    formatConfigurator.configureContextForExport(context);
    Class outputFormatClass = context.getOutputFormatClass();
    assertThat(outputFormatClass).isSameAs(SQLServerResilientExportOutputFormat.class);
  }

  @Test
  public void testNonResilientExportContextConfiguration() {
    String[] extraArgs = {"--non-resilient"};
    options.setExtraArgs(extraArgs);

    ExportJobContext context = new ExportJobContext("TABLE_NAME", "example.jar", options);
    formatConfigurator.configureContextForExport(context);
    Class outputFormatClass = context.getOutputFormatClass();
    assertThat(outputFormatClass).isSameAs(SqlServerExportBatchOutputFormat.class);
  }

  @Test
  public void testResilientUpdateContextConfiguration() {
    String[] extraArgs = {"--resilient"};
    options.setExtraArgs(extraArgs);

    ExportJobContext context = new ExportJobContext("TABLE_NAME", "example.jar", options);
    formatConfigurator.configureContextForUpdate(context, null);
    Class outputFormatClass = context.getOutputFormatClass();
    assertThat(outputFormatClass).isSameAs(SQLServerResilientUpdateOutputFormat.class);
  }

  @Test
  public void testNonResilientUpdateContextConfiguration() {
    String[] extraArgs = {"--non-resilient"};
    options.setExtraArgs(extraArgs);

    ExportJobContext context = new ExportJobContext("TABLE_NAME", "example.jar", options);
    formatConfigurator.configureContextForUpdate(context, null);
    Class outputFormatClass = context.getOutputFormatClass();
    assertThat(outputFormatClass).isNull();
  }

  @Before
  public void setUp() {
    Configuration conf = new Configuration();
    this.options = new SqoopOptions(conf);
  }
}
