package org.apache.sqoop.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

/**
 * An {@link OutputFormat} that writes plain text files.
 * Only writes the key. Does not write any delimiter/newline after the key.
 */
public class BinaryKeyOutputFormat<K, V> extends FileOutputFormat<K, V> {

  /**
   * RecordWriter to write to plain text files.
   */
  public static class BinaryKeyRecordWriter<K, V> extends RecordWriter<K, V> {

    private static final String UTF8 = "UTF-8";

    protected DataOutputStream out;

    public BinaryKeyRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof BytesWritable) {
        BytesWritable to = (BytesWritable) o;
        out.write(to.getBytes(), 0, to.getLength());
      }
    }

    public synchronized void write(K key, V value) throws IOException {
      writeObject(key);
    }

    public synchronized void close(TaskAttemptContext context)
      throws IOException {
      out.close();
    }

  }

  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
    throws IOException {
    boolean isCompressed = getCompressOutput(context);
    Configuration conf = context.getConfiguration();
    String ext = "";
    CompressionCodec codec = null;
    if (isCompressed) {
      // create the named codec
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(context, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);

      ext = codec.getDefaultExtension();
    }

    Path file = getDefaultWorkFile(context, ext);
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    DataOutputStream ostream = fileOut;

    if (isCompressed) {
      ostream = new DataOutputStream(codec.createOutputStream(fileOut));
    }

    return new BinaryKeyRecordWriter<K, V>(ostream);
  }

}