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
import static org.junit.Assert.assertNotNull;

import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestMainframeDatasetPath {

	@Test
	public void testCanGetFileNameOnSequential() throws Exception {
		String dsName = "a.b.c.d";
		String expectedFileName = "d";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL);
		assertEquals(expectedFileName,p.getMainframeDatasetFileName());
	}
	
	@Test
	public void testCanGetFolderNameOnSequential() throws Exception {
		String dsName = "a.b.c.d";
		String expectedFolderName = "a.b.c";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL);
		assertEquals(expectedFolderName,p.getMainframeDatasetFolder());
	}
	
	@Test
	public void testCanGetFolderNameOnGDG() throws Exception {
		String dsName = "a.b.c.d";
		String expectedFolderName = "a.b.c.d";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG);
		assertEquals(expectedFolderName,p.getMainframeDatasetFolder());
	}
	
	@Test
	public void testFileNameIsNullOnGDG() throws Exception {
		String dsName = "a.b.c.d";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG);
		assertEquals(null,p.getMainframeDatasetFileName());
	}
	
	@Test
	public void testConstructor1() throws Exception {
		String dsName = "a.b.c.d";
		String expectedFolderName = "a.b.c";
		String expectedFileName = "d";
		Configuration conf = new Configuration();
		conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME, dsName);
		conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE, MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL);
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,conf);
		assertEquals(dsName,p.getMainframeDatasetName());
		assertEquals(MainframeDatasetType.SEQUENTIAL.toString(),p.getMainframeDatasetType().toString());
		assertEquals(expectedFolderName, p.getMainframeDatasetFolder());
		assertEquals(expectedFileName, p.getMainframeDatasetFileName());
	}
	
	@Test
	public void testConstructor2() throws Exception {
		String dsName = "a.b.c.d";
		String expectedFolderName = "a.b.c.d";
		String dsType = "p";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,dsType);
		assertEquals(expectedFolderName,p.getMainframeDatasetFolder());
	}
	
	@Test
	public void testConstructor3() {
		String dsName = "a.b.c.d";
		String expectedFolderName = "a.b.c.d";
		MainframeDatasetPath p = new MainframeDatasetPath(dsName,MainframeDatasetType.GDG);
		assertEquals(expectedFolderName, p.getMainframeDatasetFolder());
	}
	
	@Test
	public void testUninitialisedThrowsException() {
		MainframeDatasetPath p = new MainframeDatasetPath();
		try {
			p.initialisePaths();
		} catch (IllegalStateException e) {
			assertNotNull(e);
			assertEquals("Please set data set name and type first.",e.getMessage());
		}
	}
}
