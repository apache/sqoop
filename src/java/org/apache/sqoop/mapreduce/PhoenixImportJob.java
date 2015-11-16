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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.sqoop.phoenix.PhoenixConstants;
import org.apache.sqoop.phoenix.PhoenixSqoopWritable;
import org.apache.sqoop.phoenix.PhoenixUtil;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Runs an Phoenix import via DataDrivenDBInputFormat and PhoenixOutputFormat
 */
public class PhoenixImportJob  extends DataDrivenImportJob {

  public static final Log LOG = LogFactory.getLog(
          PhoenixImportJob.class.getName());

  public PhoenixImportJob(final SqoopOptions opts,
                        final ImportJobContext importContext) {
  	super(opts, importContext.getInputFormat(), importContext);
  }

  @Override
  protected void configureMapper(Job job, String tableName,
                                   String tableClassName) throws IOException {
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(PhoenixSqoopWritable.class);
    job.setMapperClass(getMapperClass());
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
  	return PhoenixImportMapper.class;
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
            throws ClassNotFoundException {
  	return PhoenixOutputFormat.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
  		String tableClassName) throws ClassNotFoundException, IOException {
    ConnManager manager = getContext().getConnManager();
  	String[] sColumnNames = null;
  	if (null == tableName) {
  		String sqlQuery = options.getSqlQuery();
  		sColumnNames = manager.getColumnNamesForQuery(sqlQuery);
  	} else {
  		if (null == options.getColumns()) {
  			sColumnNames = manager.getColumnNames(tableName);	
  		} else {
  			sColumnNames = options.getColumns();
  		}
  	}
    String columnMappings = options.getPhoenixColumnMapping();
    // if column mappings aren't provided, we assume the column names in sqoop and phoenix match.
    if (columnMappings == null || columnMappings.isEmpty()) {
    	columnMappings = Joiner.on(",").join(sColumnNames);
    }
    job.getConfiguration().set(PhoenixConstants.PHOENIX_SQOOP_COLUMNS, Joiner.on(",").join(sColumnNames));
    job.getConfiguration().set(PhoenixConstants.PHOENIX_COLUMN_MAPPING, columnMappings);
    job.setOutputFormatClass(getOutputFormatClass());
  }

  @Override
  protected void jobSetup(Job job) throws IOException, ImportException {
  	Configuration conf = job.getConfiguration();
    HBaseConfiguration.addHbaseResources(conf);
    final String tableName = options.getPhoenixTable();
    final String sColumnNames = conf.get(PhoenixConstants.PHOENIX_SQOOP_COLUMNS);
    final String columnMappings = conf.get(PhoenixConstants.PHOENIX_COLUMN_MAPPING);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(sColumnNames);
    Preconditions.checkNotNull(columnMappings);
    
    final Map<String,String> phoenixToSqoopMapping = PhoenixUtil.getPhoenixToSqoopMap(columnMappings);
    final String pColumnNames = Joiner.on(",").join(phoenixToSqoopMapping.keySet());
    boolean isValidColumns = PhoenixUtil.validateColumns(sColumnNames,columnMappings);
    if (!isValidColumns) {
    	throw new RuntimeException(String.format("Failure to map sqoop columns [%s] to phoenix columns [%s] ", sColumnNames,pColumnNames));
    }
    // set the table and columns.
    PhoenixMapReduceUtil.setOutput(job, tableName,pColumnNames);
    TableMapReduceUtil.initCredentials(job);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(conf, HTable.class);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(), PhoenixDriver.class);
    super.jobSetup(job);
  }
}
