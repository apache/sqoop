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

import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This class determines resolves FTP folder paths
 * given the data set type and the data set name
 */

public class MainframeDatasetPath {

	private static final Log LOG =
		      LogFactory.getLog(MainframeDatasetPath.class);	
	private String datasetName;
	private MainframeDatasetType datasetType;
	private String dsFolderName = null;
	private String dsFileName = null;
	
	// default constructor
	public MainframeDatasetPath(){}
	
	// constructor that takes dataset name job configuration
	public MainframeDatasetPath(String dsName, Configuration conf) throws Exception {
		String inputName
        	= conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_NAME);
		// this should always be true
		assert(inputName.equals(dsName));
		LOG.info("Datasets to transfer from: " + dsName);
		this.datasetName = dsName;
		// initialise dataset type
		String dsType = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE);
		this.setMainframeDatasetType(dsType);
		initialisePaths();
	}
	
	public MainframeDatasetPath(String dsName, MainframeDatasetType dsType) {
		this.setMainframeDatasetName(dsName);
		this.setMainframeDatasetType(dsType);
		initialisePaths();
	}
	
	public MainframeDatasetPath(String dsName, String dsType) throws ParseException {
		this.setMainframeDatasetName(dsName);
		this.setMainframeDatasetType(dsType);
		initialisePaths();
	}
	
	public void initialisePaths() throws IllegalStateException {
		if (this.datasetName == null || this.datasetType == null) {
			throw new IllegalStateException("Please set data set name and type first.");
		}
		boolean isSequentialDs = this.datasetType.equals(MainframeDatasetType.SEQUENTIAL);
	    boolean isGDG = this.datasetType.equals(MainframeDatasetType.GDG);
	    LOG.info(String.format("dsName: %s", this.datasetName));
	    LOG.info(String.format("isSequentialDs: %s isGDG: %s", isSequentialDs, isGDG));
	    if (isSequentialDs)
	    {
	      // truncate the tailing string until the dot
	      // usually dataset qualifiers are dots. eg blah1.blah2.blah3
	      // so in this case, we should return blah1.blah2
	      int lastDotIndex = this.datasetName.lastIndexOf(".");
	      // if not found, it is probably in the root
	      if (lastDotIndex == -1) { this.datasetName = ""; } else {
	        // if found, return the truncated name
	        dsFolderName = this.datasetName.substring(0, lastDotIndex);
	        if (lastDotIndex + 1 < this.datasetName.length()) {
	          dsFileName = this.datasetName.substring(lastDotIndex + 1);
	        }
	      }
	    } else {
	    	// GDG or PDS
	    	dsFolderName = this.datasetName;
	    	dsFileName = null; // handle parentheses parsing later
	    }
	}
	
	// getters and setters
	public MainframeDatasetType getMainframeDatasetType() {
		return this.datasetType;
	}
	
	public void setMainframeDatasetType(MainframeDatasetType dsType) {
		this.datasetType = dsType;
	}
	
	// overloaded setter to parse string
	public void setMainframeDatasetType(String dsType) throws ParseException {
		if (dsType.equals("s")) { this.datasetType = MainframeDatasetType.SEQUENTIAL; }
		else if (dsType.equals("p")) { this.datasetType = MainframeDatasetType.PARTITIONED; }
		else if (dsType.equals("g")) { this.datasetType = MainframeDatasetType.GDG; }
		else { throw new ParseException(String.format("Invalid data set type specified: %s",dsType), 0); }
	}
	
	public String getMainframeDatasetName() {
		return this.datasetName;
	}
	
	public void setMainframeDatasetName(String dsName) {
		this.datasetName = dsName;
	}
	
	public String getMainframeDatasetFolder() {
		return this.dsFolderName;
	}

	public String getMainframeDatasetFileName() {
		// returns filename in the folder and null if it is a GDG as this requires a file listing
		return dsFileName;
	}
}
