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

package org.apache.sqoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import com.cloudera.sqoop.SqoopOptions;
import com.google.common.base.Preconditions;

/**
 * Runs an HBase bulk import via DataDrivenDBInputFormat to the
 * HBasePutProcessor in the DelegatingOutputFormat.
 */
public class HBaseBulkImportJob extends HBaseImportJob {

  public static final Log LOG = LogFactory.getLog(
      HBaseBulkImportJob.class.getName());

  public HBaseBulkImportJob(final SqoopOptions opts,
      final ImportJobContext importContext) {
    super(opts, importContext);
  }

  @Override
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws IOException {
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setMapperClass(getMapperClass());
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return HBaseBulkImportMapper.class;
  }

  @Override
  protected void jobSetup(Job job) throws IOException, ImportException {
    super.jobSetup(job);

    // we shouldn't have gotten here if bulk load dir is not set
    // so let's throw a ImportException
    if(getContext().getDestination() == null){
      throw new ImportException("Can't run HBaseBulkImportJob without a " +
          "valid destination directory.");
    }

    TableMapReduceUtil.addDependencyJars(job.getConfiguration(), Preconditions.class);
    FileOutputFormat.setOutputPath(job, getContext().getDestination());
    HTable hTable = new HTable(job.getConfiguration(), options.getHBaseTable());
    HFileOutputFormat.configureIncrementalLoad(job, hTable);
  }

  /**
   * Perform the loading of Hfiles.
   */
  @Override
  protected void completeImport(Job job) throws IOException, ImportException {
    super.completeImport(job);

    FileSystem fileSystem = FileSystem.get(job.getConfiguration());

    // Make the bulk load files source directory accessible to the world
    // so that the hbase user can deal with it
    Path bulkLoadDir = getContext().getDestination();
    setPermission(fileSystem, fileSystem.getFileStatus(bulkLoadDir),
      FsPermission.createImmutable((short) 00777));

    HTable hTable = new HTable(job.getConfiguration(), options.getHBaseTable());

    // Load generated HFiles into table
    try {
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
        job.getConfiguration());
      loader.doBulkLoad(bulkLoadDir, hTable);
    }
    catch (Exception e) {
      String errorMessage = String.format("Unrecoverable error while " +
        "performing the bulk load of files in [%s]",
        bulkLoadDir.toString());
      throw new ImportException(errorMessage, e);
    }
  }

  @Override
  protected void jobTeardown(Job job) throws IOException, ImportException {
    super.jobTeardown(job);
    // Delete the hfiles directory after we are finished.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    fileSystem.delete(getContext().getDestination(), true);
  }

  /**
   * Set the file permission of the path of the given fileStatus. If the path
   * is a directory, apply permission recursively to all subdirectories and
   * files.
   *
   * @param fs         the filesystem
   * @param fileStatus containing the path
   * @param permission the permission
   * @throws java.io.IOException
   */
  private void setPermission(FileSystem fs, FileStatus fileStatus,
                             FsPermission permission) throws IOException {
    if(fileStatus.isDir()) {
      for(FileStatus file : fs.listStatus(fileStatus.getPath())){
        setPermission(fs, file, permission);
      }
    }
    fs.setPermission(fileStatus.getPath(), permission);
  }
}
