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
package org.apache.sqoop.mapreduce.db.netezza;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.sqoop.mapreduce.db.DBSplitter;
import
  org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat.DataDrivenDBInputSplit;

import com.cloudera.sqoop.config.ConfigurationHelper;

/**
 * Netezza specific splitter based on data slice id.
 */
public class NetezzaDBDataSliceSplitter implements DBSplitter {

  // Note: We have removed the throws SQLException clause as there is no
  // SQL work done in this method
  @Override
  public List<InputSplit> split(Configuration conf, ResultSet results,
      String colName) {
    // For each map we will add a split such that
    // the datasliceid % the mapper index equals the mapper index.
    // The query will only be on the lower bound where clause.
    // For upper bounds, we will specify a constant clause which always
    // evaluates to true

    int numSplits = ConfigurationHelper.getConfNumMaps(conf);
    List<InputSplit> splitList = new ArrayList<InputSplit>(numSplits);
    for (int i = 0; i < numSplits; ++i) {
      StringBuilder lowerBoundClause = new StringBuilder(128);
      lowerBoundClause.append(" datasliceid % ").append(numSplits)
          .append(" = ").append(i);
      splitList.add(new DataDrivenDBInputSplit(lowerBoundClause.toString(),
          "1 = 1"));
    }
    return splitList;
  }

}
