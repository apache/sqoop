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

package org.apache.sqoop.manager;

import java.io.IOException;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.ExportException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.mapreduce.ExportInputFormat;
import org.apache.sqoop.mapreduce.postgresql.PGBulkloadExportJob;



/**
 * Manages connections to Postgresql databases.
 */
public class PGBulkloadManager extends PostgresqlManager {

  public static final Log LOG =
      LogFactory.getLog(PGBulkloadManager.class.getName());


  public PGBulkloadManager(final SqoopOptions opts) {
    super(opts);
  }


  @Override
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    options.setStagingTableName(null);
    PGBulkloadExportJob jobbase =
        new PGBulkloadExportJob(context,
                                null,
                                ExportInputFormat.class,
                                NullOutputFormat.class);
    jobbase.runExport();
  }


  @Override
  public boolean supportsStagingForExport() {
    return false;
  }

}
