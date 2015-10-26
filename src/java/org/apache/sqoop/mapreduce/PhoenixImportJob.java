package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.sqoop.phoenix.PhoenixSqoopWritable;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import org.apache.phoenix.mapreduce.PhoenixOutputFormat;

/**
 *
 */
public class PhoenixImportJob  extends DataDrivenImportJob {

    public static final Log LOG = LogFactory.getLog(
            PhoenixImportJob.class.getName());

    public static final String PHOENIX_IMPORT_COLUMNS = "phoenix.sqoop.import.columns";
    
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
        String[] columnNames = manager.getColumnNames(tableName);
        final String phoenixColumns = options.getPhoenixColumns();
        
        if(phoenixColumns == null || phoenixColumns.length() == 0) {
        	job.getConfiguration().set(PHOENIX_IMPORT_COLUMNS, Joiner.on(",").join(columnNames).toUpperCase());
        } else {
        	// validate if the columns count match.
        	String[] phoenixColumnNames = phoenixColumns.split("\\s*,\\s*");
        	if(phoenixColumnNames.length != columnNames.length) {
        		throw new RuntimeException(String.format(" We import [%s] columns from table [%s] "
        				+ "	but are writing to [%s] columns of [%s] phoenix table", columnNames.length,tableName,phoenixColumnNames.length,options.getPhoenixTable()));
        	}
        	job.getConfiguration().set(PHOENIX_IMPORT_COLUMNS,phoenixColumns);
        }
        job.setOutputFormatClass(getOutputFormatClass());
    }

    @Override
    protected void jobSetup(Job job) throws IOException, ImportException {
    	super.jobSetup(job);
        Configuration conf = job.getConfiguration();
        final String tableName = options.getPhoenixTable();
        final String columns = conf.get(PHOENIX_IMPORT_COLUMNS);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(columns);
        
        HBaseConfiguration.addHbaseResources(conf);
        
        // set the table and columns.
        PhoenixMapReduceUtil.setOutput(job, options.getPhoenixTable(), columns);
        TableMapReduceUtil.initCredentials(job);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(conf, HTable.class);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), PhoenixDriver.class);
   }


}
