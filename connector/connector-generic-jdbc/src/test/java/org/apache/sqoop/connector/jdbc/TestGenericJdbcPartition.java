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
package org.apache.sqoop.connector.jdbc;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Types;

public class TestGenericJdbcPartition {

  @Test
  public void testSerialization() throws Exception {
    GenericJdbcPartition expectedPartition = new GenericJdbcPartition();
    expectedPartition.setCondition("TEST_CONDITION");

    expectedPartition.addParam(Types.TINYINT, 1L);
    expectedPartition.addParam(Types.SMALLINT, 2L);
    expectedPartition.addParam(Types.INTEGER, 3L);
    expectedPartition.addParam(Types.BIGINT, 4L);

    expectedPartition.addParam(Types.REAL, 1.1);
    expectedPartition.addParam(Types.FLOAT, 1.2);

    expectedPartition.addParam(Types.DOUBLE, new BigDecimal(1.3));
    expectedPartition.addParam(Types.NUMERIC, new BigDecimal(1.4));
    expectedPartition.addParam(Types.DECIMAL, new BigDecimal(1.5));

    expectedPartition.addParam(Types.BIT, true);
    expectedPartition.addParam(Types.BOOLEAN, false);

    expectedPartition.addParam(Types.DATE, java.sql.Date.valueOf("2015-12-14"));
    expectedPartition.addParam(Types.TIME, java.sql.Time.valueOf("00:00:00"));
    expectedPartition.addParam(Types.TIMESTAMP, java.sql.Timestamp.valueOf("2015-12-16 00:00:00"));

    expectedPartition.addParam(Types.CHAR, "a");
    expectedPartition.addParam(Types.VARCHAR, "aa");
    expectedPartition.addParam(Types.LONGVARCHAR, "aaa");

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);

    expectedPartition.write(dataOutput);

    GenericJdbcPartition actualPartition = new GenericJdbcPartition();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInput dataInput = new DataInputStream(byteArrayInputStream);

    actualPartition.readFields(dataInput);

    assertEquals(actualPartition, expectedPartition);
  }

}
