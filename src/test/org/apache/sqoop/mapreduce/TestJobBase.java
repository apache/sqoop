/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.config.ConfigurationConstants;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.JobBase;

public class TestJobBase {

  SqoopOptions options;
  Configuration conf;
  JobBase jobBase;
  Job job;

  @Before
  public void setUp() {
    // set Sqoop command line arguments
    options = new SqoopOptions();
    conf = options.getConf();
    jobBase = spy(new JobBase(options));
  }

  private void tmpjarsValidatingSeed(String tmpjarsInput) throws IOException {

    // call cacheJars(...)
    conf.set(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM, tmpjarsInput);
    job = jobBase.createJob(conf);
    jobBase.cacheJars(job, null);

  }

  public void tmpjarsValidatingVerif(String expectedOutput, int numWarnings) throws IOException {
    // check outputs
    assertEquals("Expected " + expectedOutput + "but received something different", expectedOutput,
        job.getConfiguration().get(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM));

    // check for warnings
    verify(jobBase, times(numWarnings))
        .warn("Empty input is invalid and was removed from tmpjars.");
  }

  @Test
  public void testTmpjarsValidatingMultipleValidInputs() throws IOException {

    String tmpjarsInput = "valid,validother";
    String expectedOutput = "valid,validother";

    tmpjarsValidatingSeed(tmpjarsInput);
    tmpjarsValidatingVerif(expectedOutput, 0);
  }

  @Test
  public void testTmpjarsValidatingFullEmptyInput() throws IOException {

    String tmpjarsInput = "";
    String expectedOutput = "";

    tmpjarsValidatingSeed(tmpjarsInput);
    tmpjarsValidatingVerif(expectedOutput, 0);
  }

  @Test
  public void testTmpjarsValidatingMixedInput() throws IOException {

    String tmpjarsInput = ",,valid,,,validother,,";
    String expectedOutput = "valid,validother";

    tmpjarsValidatingSeed(tmpjarsInput);
    tmpjarsValidatingVerif(expectedOutput, 4);
  }

}
