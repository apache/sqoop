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

package org.apache.sqoop.mapreduce.hcat;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class TestSqoopHCatImportHelper {

  private SqoopHCatImportHelper importHelper;

  @Before
  public void init() {
    importHelper = new SqoopHCatImportHelper();
  }

  @Test
  public void convertLongNumberIntoBigDecimalWithoutRounding() {
    Long input = new Long("20160523112914897");
    HiveDecimal actual = importHelper.convertNumberIntoHiveDecimal(input);
    assertEquals(new BigDecimal("20160523112914897"), actual.bigDecimalValue());

  }
  @Test
  public void convertDoubleNumberIntoBigDecimalWithoutRounding() {
    Double input = new Double("0.12345678912345678");
    HiveDecimal actual = importHelper.convertNumberIntoHiveDecimal(input);
    assertEquals(new BigDecimal("0.12345678912345678"), actual.bigDecimalValue());
  }

  @Test
  public void keepBigDecimalNumberIfInputIsBigDecimal() {
    BigDecimal input = new BigDecimal("87658675864540185.123456789123456789");
    HiveDecimal actual = importHelper.convertNumberIntoHiveDecimal(input);
    assertEquals(new BigDecimal("87658675864540185.123456789123456789"), actual.bigDecimalValue());
  }

}
