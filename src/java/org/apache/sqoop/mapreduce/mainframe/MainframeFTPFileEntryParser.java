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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.parser.ConfigurableFTPFileEntryParserImpl;

public class MainframeFTPFileEntryParser extends ConfigurableFTPFileEntryParserImpl {

	/* Volume Unit    Referred Ext Used Recfm Lrecl BlkSz Dsorg Dsname
	 * xxx300 3390   2016/05/25  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOADED
	 * x31167 Tape                                             UNLOAD.EDH.UNLOADT
	 * xxx305 3390   2016/05/23  1   45  VB    2349 27998  PS  UNLOAD.EDH.UNLOAD1
	 */

	// match Unit and Dsname
	private static String REGEX = "^\\S+\\s+(\\S+)\\s+.*?\\s+(\\S+)$";
	// match Unit, BlkSz, Dsorg, DsName
	private static String NON_TAPE_REGEX = "^\\S+\\s+(\\S+)\\s+.*?(\\d+)\\s+(\\S+)\\s+(\\S+)$";
	static final String DEFAULT_DATE_FORMAT = "yyyy/MM/dd HH:mm";
	//= "MMM d yyyy"; //Nov 9 2001

	static final String DEFAULT_RECENT_DATE_FORMAT = "MMM d HH:mm"; //Nov 9 20:06

	private static String tapeUnitString = "Tape";
	private static String unitHeaderString = "Unit";
	private static String dsNameHeaderString = "Dsname";
	private static String dsOrgPDSString = "PO";
	private static String dsOrgPDSExtendedString = "PO-E";
	private static String dsOrgSeqString = "PS";
	private static Pattern nonTapePattern = Pattern.compile(NON_TAPE_REGEX);

	private static final Log LOG = LogFactory.getLog(MainframeFTPFileEntryParser.class.getName());

	public MainframeFTPFileEntryParser() {
		super(REGEX);
		LOG.info("MainframeFTPFileEntryParser constructor");
	}

	public MainframeFTPFileEntryParser(String regex) {
		super(REGEX);
	}

	public FTPFile parseFTPEntry(String entry) {
		LOG.info("parseFTPEntry: "+entry);

        if (matches(entry)) {
        	String unit = group(1);
        	String dsName = group(2);
        	LOG.info("parseFTPEntry match: "+group(1)+" "+group(2));
        	if (unit.equals(unitHeaderString) && dsName.equals(dsNameHeaderString)) {
        		return null;
        	}
        	FTPFile file = new FTPFile();
            file.setRawListing(entry);
            file.setName(dsName);
        	// match non tape values
        	if (!unit.equals("Tape")) {
	        	Matcher m = nonTapePattern.matcher(entry);
	        	// get sizes
	        	if (m.matches()) {
	        		// PO/PO-E = PDS = directory
	        		// PS = Sequential data set = file
	        		String size = m.group(2);
	        		String dsOrg = m.group(3);
	        		file.setSize(Long.parseLong(size));
	        		LOG.info(String.format("Non tape match: %s, %s, %s", file.getName(), file.getSize(), dsOrg));
	        		if (dsOrg.equals(dsOrgPDSString) || dsOrg.equals(dsOrgPDSExtendedString)) {
	        			file.setType(FTPFile.DIRECTORY_TYPE);
	        		}
	        		if (dsOrg.equals(dsOrgSeqString)) {
	        			file.setType(FTPFile.FILE_TYPE);
	        		}
	        	}
        	} else {
        		LOG.info(String.format("Tape match: %s, %s", file.getName(), unit));
        		file.setType(FTPFile.FILE_TYPE);
        	}
        	return file;
        }
		return null;
	}
	
	@Override
	protected FTPClientConfig getDefaultConfiguration() {
		return new FTPClientConfig(FTPClientConfig.SYST_MVS,
				DEFAULT_DATE_FORMAT, null, null, null, null);
	} 
}
