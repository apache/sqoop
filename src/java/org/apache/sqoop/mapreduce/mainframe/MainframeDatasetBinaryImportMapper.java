package org.apache.sqoop.mapreduce.mainframe;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.AutoProgressMapper;

/**
 * Mapper that writes mainframe dataset records in Text format to multiple files
 * based on the key, which is the index of the datasets in the input split.
 */
public class MainframeDatasetBinaryImportMapper
  extends AutoProgressMapper<LongWritable, SqoopRecord, BytesWritable, NullWritable> {

  private static final Log LOG = LogFactory.getLog(
    MainframeDatasetImportMapper.class.getName());

  private MainframeDatasetInputSplit inputSplit;
  private MultipleOutputs<BytesWritable, NullWritable> mos;
  private long numberOfRecords;
  private BytesWritable outkey;

  public void map(LongWritable key,  SqoopRecord val, Context context)
    throws IOException, InterruptedException {
    String dataset = inputSplit.getCurrentDataset();
    byte[] bytes = (byte[])val.getFieldMap().entrySet().iterator().next().getValue();
    outkey.set(bytes,0,bytes.length);
    numberOfRecords++;
    mos.write(outkey, NullWritable.get(), dataset);
  }

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    super.setup(context);
    inputSplit = (MainframeDatasetInputSplit)context.getInputSplit();
    mos = new MultipleOutputs<BytesWritable, NullWritable>(context);
    numberOfRecords = 0;
    outkey = new BytesWritable();
  }

  @Override
  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    super.cleanup(context);
    mos.close();
    context.getCounter(
      ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS,
      ConfigurationConstants.COUNTER_MAP_OUTPUT_RECORDS)
      .increment(numberOfRecords);
  }
}