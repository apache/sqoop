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
package org.apache.sqoop.job.mr;

import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.Config;
import org.apache.sqoop.model.ConfigClass;
import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Input;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Text;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Current tests are using mockito to propagate credentials from hadoop Job object
 * to hadoop JobConf object. This implementation was chosen because it's not clear
 * how MapReduce is converting one object to another.
 */
public class TestMRConfigurationUtils {

  Job job;
  JobConf jobConfSpy;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    setUpHadoopJob();
    setUpHadoopJobConf();
  }

  public void setUpHadoopJob() throws Exception {
    job = new Job();
  }

  public void setUpHadoopJobConf() throws Exception {
    jobConfSpy = spy(new JobConf(job.getConfiguration()));
    when(jobConfSpy.getCredentials()).thenReturn(job.getCredentials());
  }

  @Test
  public void testLinkConfiguration() throws Exception {
    MRConfigurationUtils.setConnectorLinkConfig(Direction.FROM, job, getConfig());
    setUpHadoopJobConf();
    assertEquals(getConfig(), MRConfigurationUtils.getConnectorLinkConfig(Direction.FROM, jobConfSpy));

    MRConfigurationUtils.setConnectorLinkConfig(Direction.TO, job, getConfig());
    setUpHadoopJobConf();
    assertEquals(getConfig(), MRConfigurationUtils.getConnectorLinkConfig(Direction.TO, jobConfSpy));
  }

  @Test
  public void testJobConfiguration() throws Exception {
    MRConfigurationUtils.setConnectorJobConfig(Direction.FROM, job, getConfig());
    setUpHadoopJobConf();
    assertEquals(getConfig(), MRConfigurationUtils.getConnectorJobConfig(Direction.FROM, jobConfSpy));

    MRConfigurationUtils.setConnectorJobConfig(Direction.TO, job, getConfig());
    setUpHadoopJobConf();
    assertEquals(getConfig(), MRConfigurationUtils.getConnectorJobConfig(Direction.TO, jobConfSpy));
  }

  @Test
  public void testDriverConfiguration() throws Exception {
    MRConfigurationUtils.setDriverConfig(job, getConfig());
    setUpHadoopJobConf();
    assertEquals(getConfig(), MRConfigurationUtils.getDriverConfig(jobConfSpy));
  }

  @Test
  public void testConnectorSchema() throws Exception {
    MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, getSchema("a"));
    assertEquals(getSchema("a"), MRConfigurationUtils.getConnectorSchema(Direction.FROM, jobConfSpy));

    MRConfigurationUtils.setConnectorSchema(Direction.TO, job, getSchema("b"));
    assertEquals(getSchema("b"), MRConfigurationUtils.getConnectorSchema(Direction.TO, jobConfSpy));
  }

  @Test
  public void testConnectorSchemaNull() throws Exception {
    MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, null);
    assertEquals(NullSchema.getInstance(),
        MRConfigurationUtils.getConnectorSchema(Direction.FROM, jobConfSpy));

    MRConfigurationUtils.setConnectorSchema(Direction.TO, job, null);
    assertEquals(NullSchema.getInstance(),
        MRConfigurationUtils.getConnectorSchema(Direction.FROM, jobConfSpy));
  }

  private Schema getSchema(String name) {
    return new Schema(name).addColumn(new Text("c1"));
  }

  private TestConfiguration getConfig() {
    TestConfiguration c = new TestConfiguration();
    c.c.A = "This is secret text!";
    return c;
  }

  @ConfigClass
  public static class C {

    @Input String A;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof C)) return false;

      C c = (C) o;

      if (A != null ? !A.equals(c.A) : c.A != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return A != null ? A.hashCode() : 0;
    }
  }

  @ConfigurationClass
  public static class TestConfiguration {
    @Config C c;

    public TestConfiguration() {
      c = new C();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestConfiguration)) return false;

      TestConfiguration config = (TestConfiguration) o;

      if (c != null ? !c.equals(config.c) : config.c != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return c != null ? c.hashCode() : 0;
    }
  }
}
