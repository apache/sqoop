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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.GenericJdbcConnectorError;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

public class GenericJdbcPartitioner extends Partitioner<LinkConfiguration, FromJobConfiguration> {

  private static final BigDecimal NUMERIC_MIN_INCREMENT = BigDecimal.valueOf(10000 * Double.MIN_VALUE);


  private long numberPartitions;
  private String partitionColumnName;
  private int partitionColumnType;
  private String partitionMinValue;
  private String partitionMaxValue;
  private Boolean allowNullValueInPartitionColumn;

  @Override
  public List<Partition> getPartitions(PartitionerContext context, LinkConfiguration linkConfig,
                                       FromJobConfiguration fromJobConfig) {
    List<Partition> partitions = new LinkedList<Partition>();

    numberPartitions = context.getMaxPartitions();
    partitionColumnName = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME);
    partitionColumnType = context.getInt(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE, -1);
    partitionMinValue = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE);
    partitionMaxValue = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE);

    allowNullValueInPartitionColumn = fromJobConfig.fromJobConfig.allowNullValueInPartitionColumn;
    if (allowNullValueInPartitionColumn == null) {
      allowNullValueInPartitionColumn = false;
    }

    if (partitionMinValue == null && partitionMaxValue == null) {
      GenericJdbcPartition partition = new GenericJdbcPartition();
      partition.setCondition(partitionColumnName + " IS NULL");
      partitions.add(partition);
      return partitions;
    }

    if (allowNullValueInPartitionColumn) {
      GenericJdbcPartition partition = new GenericJdbcPartition();
      partition.setCondition(partitionColumnName + " IS NULL");
      partitions.add(partition);
      if (numberPartitions > 1) {
        numberPartitions -= 1;
      }
      else {
        context.setSkipMaxPartitionCheck(true);
      }
    }

    switch (partitionColumnType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        // Integer column
        partitions.addAll(partitionIntegerColumn(partitionColumnType));
        break;

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
        // Floating point column
        partitions.addAll(partitionFloatingPointColumn(partitionColumnType));
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        // Decimal column
        partitions.addAll(partitionNumericColumn(partitionColumnType));
        break;

      case Types.BIT:
      case Types.BOOLEAN:
        // Boolean column
        return partitionBooleanColumn();

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        // Date time column
        partitions.addAll(partitionDateTimeColumn());
        break;

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        // Text column
        partitions.addAll(partitionTextColumn());
        break;

      default:
        throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0011,
          String.valueOf(partitionColumnType));
    }

    return partitions;
  }

  protected List<Partition> partitionDateTimeColumn() {
    List<Partition> partitions = new LinkedList<Partition>();

    long minDateValue = Long.parseLong(partitionMinValue);
    long maxDateValue = Long.parseLong(partitionMaxValue);

    long interval =  (maxDateValue - minDateValue) / numberPartitions;
    long remainder = (maxDateValue - minDateValue) % numberPartitions;

    if (interval == 0) {
      numberPartitions = (int)remainder;
    }

    long lowerBound;
    long upperBound = minDateValue;

    GenericJdbcPartition partition;
    for (int i = 1; i < numberPartitions; i++) {
      lowerBound = upperBound;
      upperBound = lowerBound + interval;
      upperBound += (i <= remainder) ? 1 : 0;

      partition = new GenericJdbcPartition();
      switch(partitionColumnType) {
        case Types.DATE:
          constructDateConditions(partition, Types.DATE, new Date(lowerBound), new Date(upperBound), false);
          break;
        case Types.TIME:
          constructDateConditions(partition, Types.TIME, new Time(lowerBound), new Time(upperBound), false);
          break;
        // Here should be the type of Types.TIMESTAMP:
        default:
          constructDateConditions(partition, Types.TIMESTAMP, new Timestamp(lowerBound), new Timestamp(upperBound), false);
          break;
      }
      partitions.add(partition);
    }

    partition = new GenericJdbcPartition();
    switch(partitionColumnType) {
      case Types.DATE:
        constructDateConditions(partition, Types.DATE, new Date(upperBound), new Date(maxDateValue), true);
        break;
      case Types.TIME:
        constructDateConditions(partition, Types.TIME, new Time(upperBound), new Time(maxDateValue), true);
        break;
      // Here should be the type of Types.TIMESTAMP:
      default:
        constructDateConditions(partition, Types.TIMESTAMP, new Timestamp(upperBound), new Timestamp(maxDateValue), true);
        break;
    }
    partitions.add(partition);
    return partitions;
  }

  protected List<Partition> partitionTextColumn() {
    List<Partition> partitions = new LinkedList<Partition>();

    String minStringValue = null;
    String maxStringValue = null;

    // Remove common prefix if any as it does not affect outcome.
    int maxPrefixLen = Math.min(partitionMinValue.length(),
      partitionMaxValue.length());
    // Calculate common prefix length
    int cpLen = 0;

    for (cpLen = 0; cpLen < maxPrefixLen; cpLen++) {
      char c1 = partitionMinValue.charAt(cpLen);
      char c2 = partitionMaxValue.charAt(cpLen);
      if (c1 != c2) {
        break;
      }
    }

    // The common prefix has length 'sharedLen'. Extract it from both.
    String prefix = partitionMinValue.substring(0, cpLen);
    minStringValue = partitionMinValue.substring(cpLen);
    maxStringValue = partitionMaxValue.substring(cpLen);

    BigDecimal minStringBD = textToBigDecimal(minStringValue);
    BigDecimal maxStringBD = textToBigDecimal(maxStringValue);

    // Having one single value means that we can create only one single split
    if(minStringBD.equals(maxStringBD)) {
      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructTextConditions(partition, prefix, 0, 0,
        partitionMinValue, partitionMaxValue, true, true);
      partitions.add(partition);
      return partitions;
    }

    // Get all the split points together.
    List<BigDecimal> splitPoints = new LinkedList<BigDecimal>();

    BigDecimal splitSize = divide(maxStringBD.subtract(minStringBD),
      new BigDecimal(numberPartitions));
    if (splitSize.compareTo(NUMERIC_MIN_INCREMENT) < 0) {
      splitSize = NUMERIC_MIN_INCREMENT;
    }

    BigDecimal curVal = minStringBD;

    int parts = 0;

    while (curVal.compareTo(maxStringBD) <= 0 && parts < numberPartitions) {
      splitPoints.add(curVal);
      curVal = curVal.add(splitSize);
      // bigDecimalToText approximates to next comparison location.
      // Make sure we are still in range
      String text = bigDecimalToText(curVal);
      curVal = textToBigDecimal(text);
      ++parts;
    }

    if (splitPoints.size() == 0
      || splitPoints.get(0).compareTo(minStringBD) != 0) {
      splitPoints.add(0, minStringBD);
    }

    if (splitPoints.get(splitPoints.size() - 1).compareTo(maxStringBD) != 0
      || splitPoints.size() == 1) {
      splitPoints.add(maxStringBD);
    }

    // Turn the split points into a set of string intervals.
    BigDecimal start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      BigDecimal end = splitPoints.get(i);

      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructTextConditions(partition, prefix, start, end,
        partitionMinValue, partitionMaxValue, i == 1, i == splitPoints.size() - 1);
      partitions.add(partition);

      start = end;
    }

    return partitions;
  }


  protected List<Partition> partitionIntegerColumn(int sqlType) {
    List<Partition> partitions = new LinkedList<Partition>();

    long minValue = partitionMinValue == null ? Long.MIN_VALUE
      : Long.parseLong(partitionMinValue);
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

      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructConditions(partition, sqlType, lowerBound, upperBound, false);
      partitions.add(partition);
    }

    GenericJdbcPartition partition = new GenericJdbcPartition();
    constructConditions(partition, sqlType, upperBound, maxValue, true);
    partitions.add(partition);

    return partitions;
  }

  protected List<Partition> partitionFloatingPointColumn(int sqlType) {
    List<Partition> partitions = new LinkedList<Partition>();


    double minValue = partitionMinValue == null ? Double.MIN_VALUE
      : Double.parseDouble(partitionMinValue);
    double maxValue = Double.parseDouble(partitionMaxValue);

    double interval =  (maxValue - minValue) / numberPartitions;

    double lowerBound;
    double upperBound = minValue;
    for (int i = 1; i < numberPartitions; i++) {
      lowerBound = upperBound;
      upperBound = lowerBound + interval;

      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructConditions(partition, sqlType, lowerBound, upperBound, false);
      partitions.add(partition);
    }

    GenericJdbcPartition partition = new GenericJdbcPartition();
    constructConditions(partition, sqlType, upperBound, maxValue, true);
    partitions.add(partition);

    return partitions;
  }

  protected List<Partition> partitionNumericColumn(int sqlType) {
    List<Partition> partitions = new LinkedList<Partition>();
    // Having one end in null is not supported
    if (partitionMinValue == null || partitionMaxValue == null) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0015);
    }

    BigDecimal minValue = new BigDecimal(partitionMinValue);
    BigDecimal maxValue = new BigDecimal(partitionMaxValue);

    // Having one single value means that we can create only one single split
    if(minValue.equals(maxValue)) {
      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructConditions(partition, sqlType, minValue);
      partitions.add(partition);
      return partitions;
    }

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
    BigDecimal end = splitPoints.get(0);
    for (int i = 0; i < numberPartitions - 1; i++) {
      BigDecimal start = splitPoints.get(i);
      end = splitPoints.get(i + 1);

      GenericJdbcPartition partition = new GenericJdbcPartition();
      constructConditions(partition, sqlType, start, end, false);
      partitions.add(partition);
    }

    GenericJdbcPartition partition = new GenericJdbcPartition();
    constructConditions(partition, sqlType, end, maxValue, true);
    partitions.add(partition);

    return partitions;
  }

  protected  List<Partition> partitionBooleanColumn() {
    List<Partition> partitions = new LinkedList<Partition>();


    Boolean minValue = parseBooleanValue(partitionMinValue);
    Boolean maxValue = parseBooleanValue(partitionMaxValue);

    StringBuilder conditions = new StringBuilder();

    // Having one single value means that we can create only one single split
    if(minValue.equals(maxValue)) {
      GenericJdbcPartition partition = new GenericJdbcPartition();

      conditions.append(partitionColumnName).append(" = ?");
      partition.setCondition(conditions.toString());
      partition.addParam(Types.BOOLEAN, maxValue);
      partitions.add(partition);
      return partitions;
    }

    GenericJdbcPartition partition = new GenericJdbcPartition();

    if (partitionMinValue == null) {
      conditions = new StringBuilder();
      conditions.append(partitionColumnName).append(" IS NULL");
      partition.setCondition(conditions.toString());
      partitions.add(partition);
    }
    partition = new GenericJdbcPartition();
    conditions = new StringBuilder();
    conditions.append(partitionColumnName).append(" = TRUE");
    partition.setCondition(conditions.toString());
    partitions.add(partition);
    partition = new GenericJdbcPartition();
    conditions = new StringBuilder();
    conditions.append(partitionColumnName).append(" = FALSE");
    partition.setCondition(conditions.toString());
    partitions.add(partition);
    return partitions;
  }

  private Boolean parseBooleanValue(String value) {
    if ("1".equals(value)) {
      return Boolean.TRUE;
    } else if ("0".equals(value)) {
      return Boolean.FALSE;
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  protected BigDecimal divide(BigDecimal numerator, BigDecimal denominator) {
    try {
      return numerator.divide(denominator);
    } catch (ArithmeticException ae) {
      return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
    }
  }

  protected void constructConditions(
    GenericJdbcPartition partition, int sqlType, Object lowerBound, Object upperBound, boolean lastOne) {
    StringBuilder conditions = new StringBuilder();
    partition.addParam(sqlType, lowerBound);
    conditions.append("? <= ");
    conditions.append(partitionColumnName);
    conditions.append(" AND ");
    conditions.append(partitionColumnName);
    conditions.append(lastOne ? " <= ?" : " < ?");
    partition.addParam(sqlType, upperBound);

    partition.setCondition(conditions.toString());
  }

  protected void constructConditions(GenericJdbcPartition partition, int sqlType, Object value) {
    String condition = partitionColumnName + " = ?";
    partition.addParam(sqlType, value);
    partition.setCondition(condition);
  }

  protected void constructDateConditions(GenericJdbcPartition partition, int dateType,
                                         Object lowerBound, Object upperBound, boolean lastOne) {
    StringBuilder conditions = new StringBuilder();
    partition.addParam(dateType, lowerBound);
    conditions.append("? <= ");
    conditions.append(partitionColumnName);
    conditions.append(" AND ");
    conditions.append(partitionColumnName);
    conditions.append(lastOne ? " <= ?" : " < ?");
    partition.setCondition(conditions.toString());
    partition.addParam(dateType, upperBound);
  }

  protected void constructTextConditions(GenericJdbcPartition partition, String prefix, Object lowerBound, Object upperBound,
                                         String lowerStringBound, String upperStringBound, boolean firstOne, boolean lastOne) {
    StringBuilder conditions = new StringBuilder();
    String lbString = prefix + bigDecimalToText((BigDecimal)lowerBound);
    String ubString = prefix + bigDecimalToText((BigDecimal)upperBound);
    partition.addParam(Types.VARCHAR, firstOne ? lowerStringBound : lbString);
    conditions.append("? <= ");
    conditions.append(partitionColumnName);
    conditions.append(" AND ");
    conditions.append(partitionColumnName);
    conditions.append(lastOne ? " <= ?" : " < ?");
    partition.addParam(Types.VARCHAR, lastOne ? upperStringBound : ubString);

    partition.setCondition(conditions.toString());
  }

  /**
   *  Converts a string to a BigDecimal representation in Base 2^21 format.
   *  The maximum Unicode code point value defined is 10FFFF.  Although
   *  not all database system support UTF16 and mostly we expect UCS2
   *  characters only, for completeness, we assume that all the unicode
   *  characters are supported.
   *  Given a string 's' containing characters s_0, s_1,..s_n,
   *  the string is interpreted as the number: 0.s_0 s_1 s_2 s_3 s_48)
   *  This can be split and each split point can be converted back to
   *  a string value for comparison purposes.   The number of characters
   *  is restricted to prevent repeating fractions and rounding errors
   *  towards the higher fraction positions.
   */
  private static final BigDecimal UNITS_BASE = new BigDecimal(0x200000);
  private static final int MAX_CHARS_TO_CONVERT = 4;

  private BigDecimal textToBigDecimal(String str) {
    BigDecimal result = BigDecimal.ZERO;
    BigDecimal divisor = UNITS_BASE;

    int len = Math.min(str.length(), MAX_CHARS_TO_CONVERT);

    for (int n = 0; n < len; ) {
      int codePoint = str.codePointAt(n);
      n += Character.charCount(codePoint);
      BigDecimal val = divide(new BigDecimal(codePoint), divisor);
      result = result.add(val);
      divisor = divisor.multiply(UNITS_BASE);
    }

    return result;
  }

  private String bigDecimalToText(BigDecimal bd) {
    BigDecimal curVal = bd.stripTrailingZeros();
    StringBuilder sb = new StringBuilder();

    for (int n = 0; n < MAX_CHARS_TO_CONVERT; ++n) {
      curVal = curVal.multiply(UNITS_BASE);
      int cp = curVal.intValue();
      if (0 >= cp) {
        break;
      }

      if (!Character.isDefined(cp)) {
        int t_cp = Character.MAX_CODE_POINT < cp ? 1 : cp;
        // We are guaranteed to find at least one character
        while(!Character.isDefined(t_cp)) {
          ++t_cp;
          if (t_cp == cp) {
            break;
          }
          if (t_cp >= Character.MAX_CODE_POINT || t_cp <= 0)  {
            t_cp = 1;
          }
        }
        cp = t_cp;
      }
      curVal = curVal.subtract(new BigDecimal(cp));
      sb.append(Character.toChars(cp));
    }

    return sb.toString();
  }
}
