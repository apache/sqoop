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

package org.apache.sqoop.hive;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.sqoop.hive.HiveTypes.toHiveType;
import static org.junit.Assert.assertEquals;
import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createEnum;
import static org.apache.avro.Schema.createFixed;
import static org.apache.avro.Schema.createUnion;
import static org.apache.avro.Schema.Type;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestHiveTypesForAvroTypeMapping {

  private final String hiveType;
  private final Schema schema;
  private final SqoopOptions options;

  @Parameters(name = "hiveType = {0}, schema = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(
        new Object[]{"BOOLEAN", create(Type.BOOLEAN), new SqoopOptions()},
        new Object[]{"INT", create(Type.INT), new SqoopOptions()},
        new Object[]{"BIGINT", create(Type.LONG), new SqoopOptions()},
        new Object[]{"FLOAT", create(Type.FLOAT), new SqoopOptions()},
        new Object[]{"DOUBLE", create(Type.DOUBLE), new SqoopOptions()},
        new Object[]{"STRING", createEnum("ENUM", "doc", "namespace", new ArrayList<>()), new SqoopOptions()},
        new Object[]{"STRING", create(Type.STRING), new SqoopOptions()},
        new Object[]{"BINARY", create(Type.BYTES), new SqoopOptions()},
        new Object[]{"BINARY", createFixed("Fixed", "doc", "space", 1), new SqoopOptions()},
        new Object[]{"BINARY", createDecimal(20, 10), new SqoopOptions()},
        new Object[]{"BINARY", create(Type.BYTES), createSqoopOptionsWithLogicalTypesEnabled()},
        new Object[]{"DECIMAL (20, 10)", createDecimal(20, 10), createSqoopOptionsWithLogicalTypesEnabled()}
        );
  }

  private static SqoopOptions createSqoopOptionsWithLogicalTypesEnabled() {
    SqoopOptions sqoopOptions = new SqoopOptions();
    sqoopOptions.getConf().setBoolean(ConfigurationConstants.PROP_ENABLE_PARQUET_LOGICAL_TYPE_DECIMAL, true);
    return sqoopOptions;
  }

  private static Schema createDecimal(int precision, int scale) {
    List<Schema> childSchemas = new ArrayList<>();
    childSchemas.add(create(Type.NULL));
    childSchemas.add(
        LogicalTypes.decimal(precision, scale)
            .addToSchema(create(Type.BYTES))
    );
    return createUnion(childSchemas);
  }

  public TestHiveTypesForAvroTypeMapping(String hiveType, Schema schema, SqoopOptions options) {
    this.hiveType = hiveType;
    this.schema = schema;
    this.options = options;
  }

  @Test
  public void testAvroTypeToHiveTypeMapping() {
    assertEquals(hiveType, toHiveType(schema, options));
  }
}
