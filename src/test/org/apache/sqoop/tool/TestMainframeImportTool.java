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

package org.apache.sqoop.tool;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cli.RelatedOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;

public class TestMainframeImportTool extends BaseSqoopTestCase {

  private static final Log LOG = LogFactory.getLog(TestMainframeImportTool.class
      .getName());

  private MainframeImportTool mfImportTool;

  @Before
  public void setUp() {

    mfImportTool = new MainframeImportTool();
  }

  @After
  public void tearDown() {
    System.setOut(null);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetImportOptions() throws SecurityException,
      NoSuchMethodException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException {
    // To access protected method by means of reflection
    Class[] types = {};
    Object[] params = {};
    Method m_getImportOptions = MainframeImportTool.class.getDeclaredMethod(
        "getImportOptions", types);
    m_getImportOptions.setAccessible(true);
    RelatedOptions rOptions = (RelatedOptions) m_getImportOptions.invoke(
        mfImportTool, params);
    assertNotNull("It should return a RelatedOptions", rOptions);
    assertTrue(rOptions.hasOption(MainframeImportTool.DS_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.DELETE_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.TARGET_DIR_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.WAREHOUSE_DIR_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.FMT_TEXTFILE_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.NUM_MAPPERS_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.MAPREDUCE_JOB_NAME));
    assertTrue(rOptions.hasOption(MainframeImportTool.COMPRESS_ARG));
    assertTrue(rOptions.hasOption(MainframeImportTool.COMPRESSION_CODEC_ARG));
  }

  @Test
  public void testApplyOptions()
      throws InvalidOptionsException, ParseException {
    String[] args = { "--" + MainframeImportTool.DS_ARG, "dummy_ds" };
    ToolOptions toolOptions = new ToolOptions();
    SqoopOptions sqoopOption = new SqoopOptions();
    mfImportTool.configureOptions(toolOptions);
    sqoopOption = mfImportTool.parseArguments(args, null, sqoopOption, false);
    assertEquals(sqoopOption.getConnManagerClassName(),
        "org.apache.sqoop.manager.MainframeManager");
    assertEquals(sqoopOption.getTableName(), "dummy_ds");
  }

  @Test
  public void testNotApplyOptions() throws ParseException,
      InvalidOptionsException {
    String[] args = new String[] { "--connection-manager=dummy_ClassName" };
    ToolOptions toolOptions = new ToolOptions();
    SqoopOptions sqoopOption = new SqoopOptions();
    mfImportTool.configureOptions(toolOptions);
    sqoopOption = mfImportTool.parseArguments(args, null, sqoopOption, false);
    assertEquals(sqoopOption.getConnManagerClassName(), "dummy_ClassName");
    assertNull(sqoopOption.getTableName());
  }
}
