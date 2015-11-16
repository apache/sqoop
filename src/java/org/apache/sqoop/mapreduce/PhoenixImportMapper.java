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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.sqoop.phoenix.PhoenixConstants;
import org.apache.sqoop.phoenix.PhoenixSqoopWritable;
import org.apache.sqoop.phoenix.PhoenixUtil;

import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;
import com.google.common.collect.Lists;

/**
 * Imports records by writing them to Phoenix 
 * 
 */
public class PhoenixImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord,NullWritable ,PhoenixSqoopWritable> {
    
	public static final Log LOG = LogFactory.getLog(
			PhoenixImportMapper.class.getName());
	
	private Configuration conf;
	private List<ColumnInfo> columnInfos;
	/* holds the mapping of phoenix column to db column */
	private Map<String,String> columnMappings;
	
	@Override
	protected void setup(Mapper<LongWritable, SqoopRecord, NullWritable, PhoenixSqoopWritable>.Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		try {
			columnInfos = PhoenixConfigurationUtil.getUpsertColumnMetadataList(conf);
			String columnMaps = conf.get(PhoenixConstants.PHOENIX_COLUMN_MAPPING);
			columnMappings = PhoenixUtil.getPhoenixToSqoopMap(columnMaps);
		} catch (SQLException e) {
			 throw new RuntimeException("Failed to load the upsert column metadata for table.");
		}
	}

	@Override
	public void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {
   
		Map<String,Object> fields = val.getFieldMap();
		PhoenixSqoopWritable recordWritable = new PhoenixSqoopWritable();
		recordWritable.setColumnMetadata(columnInfos);
		List<Object> columnValues = Lists.newArrayListWithCapacity(columnInfos.size());
		for (ColumnInfo column : columnInfos) {
			String pColName = column.getDisplayName();
			String sColName = columnMappings.get(pColName);
			Object sColValue = fields.get(sColName);
			columnValues.add(sColValue);
		}
		recordWritable.setValues(columnValues);
		context.write(NullWritable.get(), recordWritable);
	}
}
