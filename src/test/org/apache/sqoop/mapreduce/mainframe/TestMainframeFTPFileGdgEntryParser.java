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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class TestMainframeFTPFileGdgEntryParser {
  /* Sample FTP listing
  Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname
  H19761 Tape                                             G0034V00
  H81751 Tape                                             G0035V00
  H73545 Tape                                             G0036V00
  G10987 Tape                                             G0037V00
  SHT331 3390   **NONE**    1   15  VB     114 27998  PS  DUMMY
  SHT337 3390   **NONE**    1   15  VB     114 27998  PS  G0035V00.COPY
  SHT33A 3390   **NONE**    1   15  VB     114 27998  PS  HELLO

  * And what we need to get back from parsing are the following entries:-
  H19761 Tape                                             G0034V00
  H81751 Tape                                             G0035V00
  H73545 Tape                                             G0036V00
  G10987 Tape                                             G0037V00
  */
  private final static String FTP_LIST_HEADER = "Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname";
  private final String DSNAME = "G0034V00";
  private final String ENTRY = String.format("H19761 Tape                                             %s",DSNAME);
  private List<String> listing;
  private MainframeFTPFileGdgEntryParser parser;
  @Before
  public void setUpBefore() throws Exception {
    parser = new MainframeFTPFileGdgEntryParser();
    listing = new ArrayList<>();
    listing.add("Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname");
    listing.add(ENTRY);
    listing.add("H81751 Tape                                             G0035V00");
    listing.add("H73545 Tape                                             G0036V00");
    listing.add("G10987 Tape                                             G0037V00");
    listing.add("SHT331 3390   **NONE**    1   15  VB     114 27998  PS  DUMMY");
    listing.add("SHT337 3390   **NONE**    1   15  VB     114 27998  PS  G0035V00.COPY");
    listing.add("SHT33A 3390   **NONE**    1   15  VB     114 27998  PS  HELLO");
  }

  @Test
  public void testIsHeader() {
    assertTrue(parser.isFtpListingHeader(FTP_LIST_HEADER));
  }

  @Test
  public void testCreateFtpFile() {
    FTPFile file = parser.createFtpFile(ENTRY, DSNAME);
    assertEquals(ENTRY,file.getRawListing());
    assertEquals(DSNAME,file.getName());
  }

  @Test
  public void testParseFTPEntry() {
    final int EXPECTED_RECORD_COUNT=4;
    long i = listing.stream()
        .map(parser::parseFTPEntry)
        .filter(Objects::nonNull)
        .count();
    assertEquals(EXPECTED_RECORD_COUNT,i);
  }
}
