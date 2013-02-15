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

import java.math.BigDecimal;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

public class GenericJdbcImportPartitioner extends Partitioner<ConnectionConfiguration, ImportJobConfiguration> {

  private static final BigDecimal NUMERIC_MIN_INCREMENT = new BigDecimal(10000 * Double.MIN_VALUE);

  private long numberPartitions;
  private String partitionColumnName;
  private int partitionColumnType;
  private String partitionMinValue;
  private String partitionMaxValue;

  @Override
  public List<Partition> getPartitions(PartitionerContext context,ConnectionConfiguration connection, ImportJobConfiguration job) {
    numberPartitions = context.getMaxPartitions();
    partitionColumnName = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME);
    partitionColumnType = context.getInt(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE, -1);
    partitionMinValue = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE);
    partitionMaxValue = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE);

    switch (partitionColumnType) {
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      // Integer column
      return partitionIntegerColumn();

    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
      // Floating point column
      return partitionFloatingPointColumn();

    case Types.NUMERIC:
    case Types.DECIMAL:
      // Decimal column
      return partitionNumericColumn();

    case Types.BIT:
    case Types.BOOLEAN:
      // Boolean column
      // TODO: Add partition function

    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      // Date time column
      // TODO: Add partition function

    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
      // Text column
      // TODO: Add partition function

    default:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0011,
          String.valueOf(partitionColumnType));
    }
  }

  protected List<Partition> partitionIntegerColumn() {
    List<Partition> partitions = new LinkedList<Partition>();

    if (partitionMinValue == null && partitionMaxValue == null) {
      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(partitionColumnName + "IS NULL");
      partitions.add(partition);
      return partitions;
    }

    long minValue = Long.parseLong(partitionMinValue);
    long maxValue = Long.parseLong(partitionMaxValue);

    long interval =  (maxValue - minValue) / numberPartitions;
    long remainder = (maxValue - minValue) % numberPartitions;

    if (interval == 0) {
      numberPartitions = (int)remainder;
    }

    long lowerBound;
    long upperBound = minValue;
    for (int i = 1; i < numberPartitions; i++) {
      lowerBound = upperBound;
      upperBound = lowerBound + interval;
      upperBound += (i <= remainder) ? 1 : 0;

      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(
          constructConditions(lowerBound, upperBound, false));
      partitions.add(partition);
    }

    GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
    partition.setConditions(
        constructConditions(upperBound, maxValue, true));
    partitions.add(partition);

    return partitions;
  }

  protected List<Partition> partitionFloatingPointColumn() {
    List<Partition> partitions = new LinkedList<Partition>();

    if (partitionMinValue == null && partitionMaxValue == null) {
      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(partitionColumnName + "IS NULL");
      partitions.add(partition);
      return partitions;
    }

    double minValue = Double.parseDouble(partitionMinValue);
    double maxValue = Double.parseDouble(partitionMaxValue);

    double interval =  (maxValue - minValue) / numberPartitions;

    double lowerBound;
    double upperBound = minValue;
    for (int i = 1; i < numberPartitions; i++) {
      lowerBound = upperBound;
      upperBound = lowerBound + interval;

      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(
          constructConditions(lowerBound, upperBound, false));
      partitions.add(partition);
    }

    GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
    partition.setConditions(
        constructConditions(upperBound, maxValue, true));
    partitions.add(partition);

    return partitions;
  }

  protected List<Partition> partitionNumericColumn() {
    List<Partition> partitions = new LinkedList<Partition>();

    // All null valeus will result in single partition
    if (partitionMinValue == null && partitionMaxValue == null) {
      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(partitionColumnName + "IS NULL");
      partitions.add(partition);
      return partitions;
    }

    // Having one end in null is not supported
    if (partitionMinValue == null || partitionMaxValue == null) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0015);
    }

    BigDecimal minValue = new BigDecimal(partitionMinValue);
    BigDecimal maxValue = new BigDecimal(partitionMaxValue);

    // Get all the split points together.
    List<BigDecimal> splitPoints = new LinkedList<BigDecimal>();

    BigDecimal splitSize = divide(maxValue.subtract(minValue), new BigDecimal(numberPartitions));
    if (splitSize.compareTo(NUMERIC_MIN_INCREMENT) < 0) {
      splitSize = NUMERIC_MIN_INCREMENT;
    }

    BigDecimal curVal = minValue;

    while (curVal.compareTo(maxValue) <= 0) {
      splitPoints.add(curVal);
      curVal = curVal.add(splitSize);
    }

    if (splitPoints.get(splitPoints.size() - 1).compareTo(maxValue) != 0 || splitPoints.size() == 1) {
      splitPoints.remove(splitPoints.size() - 1);
      splitPoints.add(maxValue);
    }

    // Turn the split points into a set of intervals.
    BigDecimal start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      BigDecimal end = splitPoints.get(i);

      GenericJdbcImportPartition partition = new GenericJdbcImportPartition();
      partition.setConditions(constructConditions(start, end, i == splitPoints.size() - 1));
      partitions.add(partition);

      start = end;
    }

    return partitions;
  }

  protected BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
    try {
      return numerator.divide(denominator);
    } catch (ArithmeticException ae) {
      return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
    }
  }

  protected String constructConditions(
      Object lowerBound, Object upperBound, boolean lastOne) {
    StringBuilder conditions = new StringBuilder();
    conditions.append(lowerBound);
    conditions.append(" <= ");
    conditions.append(partitionColumnName);
    conditions.append(" AND ");
    conditions.append(partitionColumnName);
    conditions.append(lastOne ? " <= " : " < ");
    conditions.append(upperBound);
    return conditions.toString();
  }
}
