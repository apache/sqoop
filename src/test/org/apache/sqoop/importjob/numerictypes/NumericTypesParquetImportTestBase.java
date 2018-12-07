/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.importjob.numerictypes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.sqoop.importjob.configuration.ParquetTestConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.NumericTypesTestUtils;
import org.apache.sqoop.util.ParquetReader;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public abstract class NumericTypesParquetImportTestBase<T extends ParquetTestConfiguration> extends NumericTypesImportTestBase<T>  {

  public static final Log LOG = LogFactory.getLog(NumericTypesParquetImportTestBase.class.getName());

  public NumericTypesParquetImportTestBase(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    super(configuration, failWithoutExtraArgs, failWithPaddingOnly);
  }

  @Override
  public ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    includeCommonOptions(builder);
    builder.withOption("as-parquetfile");
    NumericTypesTestUtils.addEnableParquetDecimal(builder);
    return builder;
  }

  @Override
  public void verify() {
    verifyParquetSchema();
    verifyParquetContent();
  }

  private void verifyParquetContent() {
    ParquetReader reader = new ParquetReader(tableDirPath);
    assertEquals(Arrays.asList(configuration.getExpectedResultsForParquet()), reader.readAllInCsvSorted());
  }

  private void verifyParquetSchema() {
    ParquetReader reader = new ParquetReader(tableDirPath);
    MessageType parquetSchema = reader.readParquetSchema();

    String[] types = configuration.getTypes();
    for (int i = 0; i < types.length; i ++) {
      String type = types[i];
      if (isNumericSqlType(type)) {
        OriginalType parquetFieldType = parquetSchema.getFields().get(i).getOriginalType();
        assertEquals(OriginalType.DECIMAL, parquetFieldType);
      }
    }
  }

  private boolean isNumericSqlType(String type) {
    return type.toUpperCase().startsWith("DECIMAL")
        || type.toUpperCase().startsWith("NUMBER")
        || type.toUpperCase().startsWith("NUMERIC");
  }
}
