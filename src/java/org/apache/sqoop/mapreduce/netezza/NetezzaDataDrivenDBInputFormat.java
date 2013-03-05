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
package org.apache.sqoop.mapreduce.netezza;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.sqoop.manager.NetezzaManager;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.sqoop.mapreduce.db.netezza.NetezzaDBDataSliceSplitter;

import com.cloudera.sqoop.config.ConfigurationHelper;

/**
 * Netezza specific DB input format.
 */
public class NetezzaDataDrivenDBInputFormat<T extends DBWritable> extends
    DataDrivenDBInputFormat<T> implements Configurable {
  private static final Log LOG = LogFactory
      .getLog(NetezzaDataDrivenDBInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    int numMappers = ConfigurationHelper.getJobNumMaps(job);

    String boundaryQuery = getDBConf().getInputBoundingQuery();
    // Resort to base class if
    // dataslice aligned import is not requested
    // Not table extract
    // No boundary query
    // Only one mapper.
    if (!getConf().getBoolean(
        NetezzaManager.NETEZZA_DATASLICE_ALIGNED_ACCESS_OPT, false)
        || getDBConf().getInputTableName() == null
        || numMappers == 1
        || (boundaryQuery != null && !boundaryQuery.isEmpty())) {
      return super.getSplits(job);
    }

    // Generate a splitter that splits only on datasliceid. It is an
    // integer split. We will just use the lower bounding query to specify
    // the restriction of dataslice and set the upper bound to a constant

    NetezzaDBDataSliceSplitter splitter = new NetezzaDBDataSliceSplitter();

    return splitter.split(getConf(), null, null);
  }
}
