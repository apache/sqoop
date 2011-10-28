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

package com.cloudera.sqoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.util.PerfCounters;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class MySQLDumpMapper
    extends org.apache.sqoop.mapreduce.MySQLDumpMapper {

  /**
   * @deprecated Moving to use org.apache.sqoop namespace.
   */
  public static class CopyingAsyncSink
      extends org.apache.sqoop.mapreduce.MySQLDumpMapper.CopyingAsyncSink {

    protected CopyingAsyncSink(final MySQLDumpMapper.Context context,
        final PerfCounters ctrs) {
      super(context, ctrs);
    }

  }

  /**
   * @deprecated Moving to use org.apache.sqoop namespace.
   */
  public static class ReparsingAsyncSink
      extends org.apache.sqoop.mapreduce.MySQLDumpMapper.ReparsingAsyncSink {

    protected ReparsingAsyncSink(final MySQLDumpMapper.Context c,
        final Configuration conf, final PerfCounters ctrs) {
      super(c, conf, ctrs);
    }

  }

}

