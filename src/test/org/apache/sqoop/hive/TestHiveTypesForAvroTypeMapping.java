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
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

import static org.apache.sqoop.hive.HiveTypes.toHiveType;
import static org.junit.Assert.*;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestHiveTypesForAvroTypeMapping {

  private final String hiveType;
  private final Schema.Type avroType;

  @Parameters(name = "hiveType = {0}, avroType = {1}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(
        new Object[] {"BOOLEAN", Schema.Type.BOOLEAN},
        new Object[] {"INT", Schema.Type.INT},
        new Object[] {"BIGINT", Schema.Type.LONG},
        new Object[] {"FLOAT", Schema.Type.FLOAT},
        new Object[] {"DOUBLE", Schema.Type.DOUBLE},
        new Object[] {"STRING", Schema.Type.ENUM},
        new Object[] {"STRING", Schema.Type.STRING},
        new Object[] {"BINARY", Schema.Type.BYTES},
        new Object[] {"BINARY", Schema.Type.FIXED});
  }

  public TestHiveTypesForAvroTypeMapping(String hiveType, Schema.Type avroType) {
    this.hiveType = hiveType;
    this.avroType = avroType;
  }

  @Test
  public void testAvroTypeToHiveTypeMapping() throws Exception {
    assertEquals(hiveType, toHiveType(avroType));
  }
}
