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

package org.apache.sqoop.mapreduce.mainframe;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ImportJobContext;

public class TestMainframeImportJob {

  private MainframeImportJob mfImportJob;

  private MainframeImportJob avroImportJob;

  private SqoopOptions options;

  @Before
  public void setUp() {
    options = new SqoopOptions();
  }

  @Test
  public void testGetMainframeDatasetImportMapperClass()
      throws SecurityException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    String jarFile = "dummyJarFile";
    String tableName = "dummyTableName";
    Path path = new Path("dummyPath");
    ImportJobContext context = new ImportJobContext(tableName, jarFile,
        options, path);
    mfImportJob = new MainframeImportJob(options, context);

    // To access protected method by means of reflection
    Class[] types = {};
    Method m_getMapperClass = MainframeImportJob.class.getDeclaredMethod(
        "getMapperClass", types);
    m_getMapperClass.setAccessible(true);
    Class<? extends Mapper> mapper = (Class<? extends Mapper>) m_getMapperClass
        .invoke(mfImportJob);
    assertEquals(mapper,
       org.apache.sqoop.mapreduce.mainframe.MainframeDatasetImportMapper.class);
  }

  @Test
  public void testSuperMapperClass() throws SecurityException,
      NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    String jarFile = "dummyJarFile";
    String tableName = "dummyTableName";
    Path path = new Path("dummyPath");
    options.setFileLayout(SqoopOptions.FileLayout.AvroDataFile);
    ImportJobContext context = new ImportJobContext(tableName, jarFile,
        options, path);
    avroImportJob = new MainframeImportJob(options, context);

    // To access protected method by means of reflection
    Class[] types = {};
    Method m_getMapperClass = MainframeImportJob.class.getDeclaredMethod(
        "getMapperClass", types);
    m_getMapperClass.setAccessible(true);
    Class<? extends Mapper> mapper = (Class<? extends Mapper>) m_getMapperClass
        .invoke(avroImportJob);
    assertEquals(mapper, org.apache.sqoop.mapreduce.AvroImportMapper.class);
  }
}
