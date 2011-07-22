/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.testutil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReporter;

/**
 * Allows the creation of various mock objects for testing purposes.
 */
public final class MockObjectFactory {

  /**
   * Returns a mock MapContext that has both an OutputCommitter and an
   * InputSplit wired to the specified path.
   * Used for testing LargeObjectLoader.
   */
  public static MapContext getMapContextForIOPath(Configuration conf, Path p) {
    return new MockMapContextWithCommitter(conf, p);
  }

  private static class MockMapContextWithCommitter
      extends MapContext<Object, Object, Object, Object> {
    private Path path;
    private Configuration conf;

    public MockMapContextWithCommitter(Configuration c, Path p) {
      super(c, new TaskAttemptID("jt", 0, true, 0, 0),
            null, null, null, new MockReporter(new Counters()), null);

      this.path = p;
      this.conf = c;
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      try {
        return new FileOutputCommitter(path, this);
      } catch (IOException ioe) {
        return null;
      }
    }

    @Override
    public InputSplit getInputSplit() {
      return new FileSplit(new Path(path, "inputFile"), 0, 0, new String[0]);
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }

  private MockObjectFactory() {
    // Disable explicity object creation
  }
}
