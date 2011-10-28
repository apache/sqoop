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

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.ProcessingException;

/**
 * OutputFormat that produces a RecordReader which instantiates
 * a FieldMapProcessor which will process FieldMappable
 * output keys.
 *
 * <p>The output value is ignored.</p>
 *
 * <p>The FieldMapProcessor implementation may do any arbitrary
 * processing on the object. For example, it may write an object
 * to HBase, etc.</p>
 *
 * <p>If the FieldMapProcessor implementation also implements
 * Closeable, it will be close()'d in the RecordReader's close()
 * method.</p>
 *
 * <p>If the FMP implements Configurable, it will be configured
 * correctly via ReflectionUtils.</p>
 */
public class DelegatingOutputFormat<K extends FieldMappable, V>
    extends OutputFormat<K, V> {

  /** conf key: the FieldMapProcessor class to instantiate. */
  public static final String DELEGATE_CLASS_KEY =
      "sqoop.output.delegate.field.map.processor.class";

  @Override
  /** {@inheritDoc} */
  public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    if (null == conf.get(DELEGATE_CLASS_KEY)) {
      throw new IOException("Delegate FieldMapProcessor class is not set.");
    }
  }

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new DelegatingRecordWriter(context);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }
  }

  /**
   * RecordWriter to write the output to a row in a database table.
   * The actual database updates are executed in a second thread.
   */
  public class DelegatingRecordWriter extends RecordWriter<K, V> {

    private Configuration conf;

    private FieldMapProcessor mapProcessor;

    public DelegatingRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException {

      this.conf = context.getConfiguration();

      @SuppressWarnings("unchecked")
      Class<? extends FieldMapProcessor> procClass =
          (Class<? extends FieldMapProcessor>)
          conf.getClass(DELEGATE_CLASS_KEY, null);
      this.mapProcessor = ReflectionUtils.newInstance(procClass, this.conf);
    }

    protected Configuration getConf() {
      return this.conf;
    }

    @Override
    /** {@inheritDoc} */
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      if (mapProcessor instanceof Closeable) {
        ((Closeable) mapProcessor).close();
      }
    }

    @Override
    /** {@inheritDoc} */
    public void write(K key, V value)
        throws InterruptedException, IOException {
      try {
        mapProcessor.accept(key);
      } catch (ProcessingException pe) {
        throw new IOException(pe);
      }
    }
  }
}
