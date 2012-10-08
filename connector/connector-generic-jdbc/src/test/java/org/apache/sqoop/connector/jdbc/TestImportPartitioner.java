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

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.MutableContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.junit.Test;

public class TestImportPartitioner extends TestCase {

  private static final int START = -5;
  private static final int NUMBER_OF_ROWS = 11;

  @Test
  public void testIntegerEvenPartition() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
        "ICOL");
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
        String.valueOf(Types.INTEGER));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
        String.valueOf(START));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
        String.valueOf(START + NUMBER_OF_ROWS - 1));
    context.setString(Constants.JOB_ETL_NUMBER_PARTITIONS, "5");

    Partitioner partitioner = new GenericJdbcImportPartitioner();
    List<Partition> partitions = partitioner.run(context);

    verifyResult(partitions, new String[] {
        "-5 <= ICOL AND ICOL < -3",
        "-3 <= ICOL AND ICOL < -1",
        "-1 <= ICOL AND ICOL < 1",
        "1 <= ICOL AND ICOL < 3",
        "3 <= ICOL AND ICOL <= 5"
    });
  }

  @Test
  public void testIntegerUnevenPartition() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
        "ICOL");
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
        String.valueOf(Types.INTEGER));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
        String.valueOf(START));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
        String.valueOf(START + NUMBER_OF_ROWS - 1));
    context.setString(Constants.JOB_ETL_NUMBER_PARTITIONS, "3");

    Partitioner partitioner = new GenericJdbcImportPartitioner();
    List<Partition> partitions = partitioner.run(context);

    verifyResult(partitions, new String[] {
        "-5 <= ICOL AND ICOL < -1",
        "-1 <= ICOL AND ICOL < 2",
        "2 <= ICOL AND ICOL <= 5"
    });
  }

  @Test
  public void testIntegerOverPartition() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
        "ICOL");
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
        String.valueOf(Types.INTEGER));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
        String.valueOf(START));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
        String.valueOf(START + NUMBER_OF_ROWS - 1));
    context.setString(Constants.JOB_ETL_NUMBER_PARTITIONS, "13");

    Partitioner partitioner = new GenericJdbcImportPartitioner();
    List<Partition> partitions = partitioner.run(context);

    verifyResult(partitions, new String[] {
        "-5 <= ICOL AND ICOL < -4",
        "-4 <= ICOL AND ICOL < -3",
        "-3 <= ICOL AND ICOL < -2",
        "-2 <= ICOL AND ICOL < -1",
        "-1 <= ICOL AND ICOL < 0",
        "0 <= ICOL AND ICOL < 1",
        "1 <= ICOL AND ICOL < 2",
        "2 <= ICOL AND ICOL < 3",
        "3 <= ICOL AND ICOL < 4",
        "4 <= ICOL AND ICOL <= 5"
    });
  }

  @Test
  public void testFloatingPointEvenPartition() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
        "DCOL");
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
        String.valueOf(Types.DOUBLE));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
        String.valueOf((double)START));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
        String.valueOf((double)(START + NUMBER_OF_ROWS - 1)));
    context.setString(Constants.JOB_ETL_NUMBER_PARTITIONS, "5");

    Partitioner partitioner = new GenericJdbcImportPartitioner();
    List<Partition> partitions = partitioner.run(context);

    verifyResult(partitions, new String[] {
        "-5.0 <= DCOL AND DCOL < -3.0",
        "-3.0 <= DCOL AND DCOL < -1.0",
        "-1.0 <= DCOL AND DCOL < 1.0",
        "1.0 <= DCOL AND DCOL < 3.0",
        "3.0 <= DCOL AND DCOL <= 5.0"
    });
  }

  @Test
  public void testFloatingPointUnevenPartition() throws Exception {
    DummyContext context = new DummyContext();
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
        "DCOL");
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
        String.valueOf(Types.DOUBLE));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
        String.valueOf((double)START));
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
        String.valueOf((double)(START + NUMBER_OF_ROWS - 1)));
    context.setString(Constants.JOB_ETL_NUMBER_PARTITIONS, "3");

    Partitioner partitioner = new GenericJdbcImportPartitioner();
    List<Partition> partitions = partitioner.run(context);

    verifyResult(partitions, new String[] {
        "-5.0 <= DCOL AND DCOL < -1.6666666666666665",
        "-1.6666666666666665 <= DCOL AND DCOL < 1.666666666666667",
        "1.666666666666667 <= DCOL AND DCOL <= 5.0"
    });
  }

  private void verifyResult(List<Partition> partitions,
      String[] expected) {
    assertEquals(expected.length, partitions.size());

    Iterator<Partition> iterator = partitions.iterator();
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i],
          ((GenericJdbcImportPartition)iterator.next()).getConditions());
    }
  }

  public class DummyContext implements MutableContext {
    HashMap<String, String> store = new HashMap<String, String>();

    @Override
    public String getString(String key) {
      return store.get(key);
    }

    @Override
    public void setString(String key, String value) {
      store.put(key, value);
    }
  }

}
