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

import org.apache.avro.Schema;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.sqoop.hive.HiveTypes.toHiveType;
import static org.junit.Assert.*;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestHiveTypesForAvroTypeMapping {

  private final String hiveType;
  private final Schema schema;

  @Parameters(name = "hiveType = {0}, schema = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(
        new Object[]{"BOOLEAN", Schema.create(Schema.Type.BOOLEAN)},
        new Object[]{"INT", Schema.create(Schema.Type.INT)},
        new Object[]{"BIGINT", Schema.create(Schema.Type.LONG)},
        new Object[]{"FLOAT", Schema.create(Schema.Type.FLOAT)},
        new Object[]{"DOUBLE", Schema.create(Schema.Type.DOUBLE)},
        new Object[]{"STRING", Schema.createEnum("ENUM", "doc", "namespce", new ArrayList<>())}, // Schema.Type.ENUM
        new Object[]{"STRING", Schema.create(Schema.Type.STRING)},
        new Object[]{"BINARY", Schema.create(Schema.Type.BYTES)},
        new Object[]{"BINARY", Schema.createFixed("Fixed", "doc", "space", 1) }
        //, new Object[]{"DECIMAL", Schema.create(Schema.Type.UNION).}
        );
  }

  public TestHiveTypesForAvroTypeMapping(String hiveType, Schema schema) {
    this.hiveType = hiveType;
    this.schema = schema;
  }

  @Test
  public void testAvroTypeToHiveTypeMapping() throws Exception {
    assertEquals(hiveType, toHiveType(schema, new SqoopOptions()));
  }
}
