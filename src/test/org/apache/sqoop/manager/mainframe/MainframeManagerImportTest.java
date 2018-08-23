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

package org.apache.sqoop.manager.mainframe;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.apache.sqoop.tool.MainframeImportTool;
import org.apache.sqoop.util.FileListing;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Mainframe integration tests
 * Can be run using -Dtestcase=MainframeManagerImportTest or -Dthirdparty=true.
 * To run tests:-
 * Start docker containers by running start-thirdpartytest-db-containers.sh
 * Run test using ant test -Dtestcase=MainframeManagerImportTest or -Dthirdparty=true
 * Clean up containers by running stop-thirdpartytest-db-containers.sh
 * The following properties can be overridden from command line
 * by setting -D<property.name>=<value>:-
 * <property name="sqoop.test.mainframe.ftp.host" value="localhost" />
 * <property name="sqoop.test.mainframe.ftp.port" value="2121" />
 * <property name="sqoop.test.mainframe.ftp.username" value="test" />
 * <property name="sqoop.test.mainframe.ftp.password" value="test" />
 * <property name="sqoop.test.mainframe.ftp.dataset.gdg" value="TSODIQ1.FOLDER" />
 * <property name="sqoop.test.mainframe.ftp.dataset.gdg.filename" value="G0001V43" />
 * <property name="sqoop.test.mainframe.ftp.dataset.gdg.md5" value="43eefbe34e466dd3f65a3e867a60809a" />
 */

public class MainframeManagerImportTest extends ImportJobTestCase {
  private static final Log LOG = LogFactory.getLog(
      MainframeManagerImportTest.class.getName());

  @Override
  protected boolean useHsqldbTestServer() {
    return false;
  }

  /** Does the import and verify
   * @param datasetName the mainframe dataset name
   * @param datasetType the mainframe dataset type from MainframeConfiguration (s/g/p)
   * @param fileHashes  each HashMap entry is filename, expected md5sum
   * @param extraArgs   extra arguments to the tool if required
   * @throws IOException if it fails to delete the directory or read the file
   * @throws RuntimeException if it fails to run the mainframe import
   */
  private void doImportAndVerify(String datasetName, String datasetType, HashMap<String,String> fileHashes, String ... extraArgs) throws IOException, RuntimeException {
    Path tablePath = new Path(datasetName);

    File tableFile = new File(tablePath.toString());
    if (tableFile.exists() && tableFile.isDirectory()) {
      // remove the directory before running the import.
      LOG.info(String.format("Removing folder: %s", tableFile));
      FileListing.recursiveDeleteDir(tableFile);
    }

    String [] argv = getArgv(datasetName, datasetType, extraArgs);
    try {
      MainframeImportTool tool = new MainframeImportTool();
      runImport(tool,argv);
    } catch (IOException ioe) {
      LOG.error("Got IOException during import: " + ioe);
      throw new RuntimeException(ioe);
    }

    Set<String> keys = fileHashes.keySet();
    for (String i : keys) {
      Path filePath = new Path(tablePath, String.format("%s%s", i,"-m-00000"));
      LOG.info(String.format("Checking for presence of file: %s with MD5 hash %s",filePath,fileHashes.get(i)));
      File f = new File(filePath.toString());
      assertTrue("Could not find imported data file", f.exists());
      FileInputStream fis = new FileInputStream(f);
      String md5 = DigestUtils.md5Hex(fis);
      fis.close();
      assertTrue(String.format("MD5 sums do not match for file: %s. Got MD5 of %s and expected %s",filePath,md5,fileHashes.get(i)),StringUtils.equalsIgnoreCase(md5, fileHashes.get(i)));
    }
  }

  @Test
  public void testImportGdgText() throws IOException {
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.GDG_DATASET_FILENAME, MainframeTestUtil.EXPECTED_GDG_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.GDG_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG, files);
  }

  @Test
  public void testImportGdgBinary() throws IOException {
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.GDG_BINARY_DATASET_FILENAME, MainframeTestUtil.EXPECTED_GDG_BINARY_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.GDG_BINARY_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG, files, "--as-binaryfile");
  }

  @Test
  public void testImportGdgBinaryWithBufferSize() throws IOException {
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.GDG_BINARY_DATASET_FILENAME, MainframeTestUtil.EXPECTED_GDG_BINARY_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.GDG_BINARY_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG, files, "--as-binaryfile", "--buffersize", "64000");
  }

  @Test
  public void testImportSequential() throws IOException {
    // can reuse the same dataset as binary as the dataset is plain text
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.SEQ_DATASET_FILENAME, MainframeTestUtil.EXPECTED_SEQ_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.SEQ_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL, files);
  }

  @Test
  public void testImportSequentialBinary() throws IOException {
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.SEQ_BINARY_DATASET_FILENAME, MainframeTestUtil.EXPECTED_SEQ_BINARY_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.SEQ_BINARY_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL, files, "--as-binaryfile");
  }

  @Test
  public void testImportSequentialBinaryWithBufferSize() throws IOException {
    HashMap<String,String> files = new HashMap<String,String>();
    files.put(MainframeTestUtil.SEQ_BINARY_DATASET_FILENAME, MainframeTestUtil.EXPECTED_SEQ_BINARY_DATASET_MD5);
    doImportAndVerify(MainframeTestUtil.SEQ_BINARY_DATASET_NAME, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL, files, "--as-binaryfile", "--buffersize", "64000");
  }

  private String [] getArgv(String datasetName, String datasetType, String ... extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--connect");
    args.add(String.format("%s:%s", MainframeTestUtil.HOST, MainframeTestUtil.PORT));
    args.add("--username");
    args.add(MainframeTestUtil.USERNAME);
    args.add("--password");
    args.add(MainframeTestUtil.PASSWORD);
    args.add("--dataset");
    args.add(datasetName);
    args.add("--datasettype");
    args.add(datasetType);

    if (extraArgs.length > 0) {
      for (String arg : extraArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }
}
