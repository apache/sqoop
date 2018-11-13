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

import org.apache.commons.net.ftp.FTPFile;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class TestMainframeFTPFileEntryParser {
	static List<String> listing;
	static MainframeFTPFileEntryParser parser2;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		/* Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname
		 * xxx300 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOADED
x31167 Tape                                                                               UNLOAD.EDH.UNLOADT
xxx305 3390   2016/05/23  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD1
xxx305 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD2
xxx305 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD3
Migrated                                                DUMMY.DATA
OVR343 3390   2018/01/23  1    1  FB     132 27984  PS  EMPTY
Migrated                                                JCL.CNTL
OVR346 3390   2018/01/22  1    1  FB      80 27920  PS  MIXED.FB80
Migrated                                                PLAIN.FB80
OVR341 3390   2018/01/23  1    9  VA     125   129  PS  PRDA.SPFLOG1.LIST
G20427 Tape                                             UNLOAD.ABCDE.ZZ9UYT.FB.TAPE
SEM352 3390   2018/01/23  1    1  FB     150  1500  PS  USER.BRODCAST
OVR346 3390   2018/01/23  3    3  FB      80  6160  PO  USER.ISPPROF
		 */
		listing = new ArrayList<String>();
		listing.add("Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname");
		listing.add("xxx300 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOADED");
		listing.add("x31167 Tape                                                                               UNLOAD.EDH.UNLOADT");
		listing.add("xxx305 3390   2016/05/23  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD1");
		listing.add("xxx305 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD2");
		listing.add("xxx305 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD3");
    listing.add("Migrated                                                PLAIN.FB80");
    listing.add("Migrated                                                DUMMY.DATA");
    listing.add("Migrated                                                JCL.CNTL");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMainframeFTPFileEntryParserString() {
		MainframeFTPFileEntryParser parser = new MainframeFTPFileEntryParser();
		assert(parser != null);
	}

	@Test
	public void testParseFTPEntry() {
		parser2 = new MainframeFTPFileEntryParser();
		int i = 0;
		for (String j : listing) {
			FTPFile file = parser2.parseFTPEntry(j);
			if (file != null) {
				i++;
			}
		}
		assert(i == listing.size()-1);
	}
}
