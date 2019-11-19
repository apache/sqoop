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

package org.apache.sqoop.mapreduce.db;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.sqoop.mapreduce.db.BigDecimalSplitter;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBigDecimalSplitter {

  private org.apache.sqoop.mapreduce.db.BigDecimalSplitter bigDecimalSplitter = new org.apache.sqoop.mapreduce.db.BigDecimalSplitter();

  /* Test if the decimal split sizes are generated as expected */
  @Test
  public void testDecimalTryDivide() {
    BigDecimal numerator = new BigDecimal("2");
    BigDecimal denominator = new BigDecimal("4");
	BigDecimal expected = new BigDecimal("0.5");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

  /* Test if the integer split sizes are generated as expected */
  @Test
  public void testIntegerTryDivide() {
	BigDecimal numerator = new BigDecimal("99");
	BigDecimal denominator = new BigDecimal("3");
	BigDecimal expected = new BigDecimal("33");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

  /* Test if the recurring decimal split sizes are generated as expected */
  @Test
  public void testRecurringTryDivide() {
	BigDecimal numerator = new BigDecimal("1");
	BigDecimal denominator = new BigDecimal("3");
	BigDecimal expected = new BigDecimal("1");
	BigDecimalSplitter splitter = new BigDecimalSplitter();
	BigDecimal out = splitter.tryDivide(numerator, denominator);
	assertEquals(expected, out);
  }

  @Test
  public void testSplit() throws SQLException {
    String colName = "cur_lac";

    BigDecimal minVal = new BigDecimal(6591);
    BigDecimal maxVal = new BigDecimal(24996);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    BigDecimal numSplits = new BigDecimal(2000);

	  if (minVal == null && maxVal == null) {
		  // Range is null to null. Return a null split accordingly.
		  List<InputSplit> splits = new ArrayList<InputSplit>();
		  splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
				  colName + " IS NULL", colName + " IS NULL"));
		  return;
	  }

	  if (minVal == null || maxVal == null) {
		  // Don't know what is a reasonable min/max value for interpolation. Fail.
          System.out.println("Cannot find a range for NUMERIC or DECIMAL "
				  + "fields with one end NULL.");
		  return;
	  }

	  // Get all the split points together.
	  List<BigDecimal> splitPoints = bigDecimalSplitter.split(numSplits, minVal, maxVal);
	  List<InputSplit> splits = new ArrayList<InputSplit>();

	  // Turn the split points into a set of intervals.
	  BigDecimal start = splitPoints.get(0);
	  for (int i = 1; i < splitPoints.size(); i++) {
		  BigDecimal end = splitPoints.get(i);

		  if (i == splitPoints.size() - 1) {
			  // This is the last one; use a closed interval.
			  splits.add(new com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit(
					  lowClausePrefix + start.toString(),
					  colName + " <= " + end.toString()));
		  } else {
			  // Normal open-interval case.
			  splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
					  lowClausePrefix + start.toString(),
					  highClausePrefix + end.toString()));
		  }
		  start = end;
	  }
  }
}
