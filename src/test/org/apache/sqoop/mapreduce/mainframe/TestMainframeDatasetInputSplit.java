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

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMainframeDatasetInputSplit {

  private MainframeDatasetInputSplit mfDatasetInputSplit;

  @Before
  public void setUp() {
    mfDatasetInputSplit = new MainframeDatasetInputSplit();
  }

  @Test
  public void testGetCurrentDataset() {
    String currentDataset = mfDatasetInputSplit.getCurrentDataset();
    Assert.assertNull(currentDataset);
  }

  @Test
  public void testGetNextDatasetWithNull() {
    String currentDataset = mfDatasetInputSplit.getNextDataset();
    Assert.assertNull(currentDataset);
  }

  @Test
  public void testGetNextDataset() {
    String mainframeDataset = "test";
    mfDatasetInputSplit.addDataset(mainframeDataset);
    String currentDataset = mfDatasetInputSplit.getNextDataset();
    Assert.assertEquals("test", currentDataset);
  }

  @Test
  public void testHasMoreWithFalse() {
    boolean retVal = mfDatasetInputSplit.hasMore();
    Assert.assertFalse(retVal);
  }

  @Test
  public void testHasMoreWithTrue() {
    String mainframeDataset = "test";
    mfDatasetInputSplit.addDataset(mainframeDataset);
    boolean retVal = mfDatasetInputSplit.hasMore();
    Assert.assertTrue(retVal);
  }

  @Test
  public void testGetLength() {
    String mainframeDataset = "test";
    mfDatasetInputSplit.addDataset(mainframeDataset);
    try {
      long retVal = mfDatasetInputSplit.getLength();
      Assert.assertEquals(1, retVal);
    } catch (IOException ioe) {
      Assert.fail("No IOException should be thrown!");
    } catch (InterruptedException ie) {
      Assert.fail("No InterruptedException should be thrown!");
    }
  }

  @Test
  public void testGetLocations() {
    try {
      String[] retVal = mfDatasetInputSplit.getLocations();
      Assert.assertNotNull(retVal);
    } catch (IOException ioe) {
      Assert.fail("No IOException should be thrown!");
    } catch (InterruptedException ie) {
      Assert.fail("No InterruptedException should be thrown!");
    }
  }

  @Test
  public void testWriteRead() {
    mfDatasetInputSplit.addDataset("dataSet1");
    mfDatasetInputSplit.addDataset("dataSet2");
    DataOutputBuffer dob = new DataOutputBuffer();
    DataInputBuffer dib = new DataInputBuffer();
    MainframeDatasetInputSplit mfReader = new MainframeDatasetInputSplit();
    try {
      mfDatasetInputSplit.write(dob);
      dib.reset(dob.getData(), dob.getLength());
      mfReader.readFields(dib);
      Assert.assertNotNull("MFReader get data from tester", mfReader);
      Assert.assertEquals(2, mfReader.getLength());
      Assert.assertEquals("dataSet1", mfReader.getNextDataset());
      Assert.assertEquals("dataSet2", mfReader.getNextDataset());
    } catch (IOException ioe) {
      Assert.fail("No IOException should be thrown!");
    } catch (InterruptedException ie) {
      Assert.fail("No InterruptedException should be thrown!");
    }
  }
}
