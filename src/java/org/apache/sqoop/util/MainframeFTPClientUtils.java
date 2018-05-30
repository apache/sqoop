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

package org.apache.sqoop.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPListParseEngine;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sqoop.mapreduce.JobBase;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.mainframe.MainframeConfiguration;
import org.apache.sqoop.mapreduce.mainframe.MainframeDatasetPath;

/**
 * Utility methods used when accessing a mainframe server through FTP client.
 */
public final class MainframeFTPClientUtils {
  private static final Log LOG = LogFactory.getLog(
      MainframeFTPClientUtils.class.getName());

  private static FTPClient mockFTPClient = null; // Used for unit testing

  private MainframeFTPClientUtils() {
  }

  public static List<String> listSequentialDatasets(String pdsName, Configuration conf) throws IOException {
    List<String> datasets = new ArrayList<String>();
    String dsName = pdsName;
    String fileName = "";
    MainframeDatasetPath p = null;
    String dsType = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE);
    if (dsType == null) {
      // default dataset type to partitioned dataset
      conf.set(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE,MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_PARTITIONED);
      dsType = conf.get(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE);
    }
    try {
    	p = new MainframeDatasetPath(dsName,conf);
    } catch (Exception e) {
    	LOG.error(e.getMessage());
    	LOG.error("MainframeDatasetPath helper class incorrectly initialised");
    	e.printStackTrace();
    }
    boolean isSequentialDs = false;
    boolean isGDG = false;
    if (dsType != null && p != null) {
    	isSequentialDs = p.getMainframeDatasetType().toString().equals(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL);
        isGDG = p.getMainframeDatasetType().toString().equals(MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG);
    	pdsName = p.getMainframeDatasetFolder();
    	fileName = p.getMainframeDatasetFileName();
    }
    FTPClient ftp = null;
    try {
      ftp = getFTPConnection(conf);
      if (ftp != null) {
        ftp.changeWorkingDirectory("'" + pdsName + "'");
        FTPFile[] ftpFiles = null;
        if (!MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_PARTITIONED.equals(dsType)) {
          // excepting partitioned datasets, use the MainframeFTPFileEntryParser, default doesn't match larger datasets
        	FTPListParseEngine parser = ftp.initiateListParsing(MainframeConfiguration.MAINFRAME_FTP_FILE_ENTRY_PARSER_CLASSNAME, "");
        	List<FTPFile> listing = new ArrayList<FTPFile>();
        	while(parser.hasNext()) {
        		FTPFile[] files = parser.getNext(25);
        		for (FTPFile file : files) {
        			if (file != null) {
        				listing.add(file);
        				LOG.info(String.format("Name: %s Type: %s", file.getName(), file.getType()));
        			}
        			// skip nulls returned from parser
        		}
        		ftpFiles = new FTPFile[listing.size()];
        		for (int i = 0;i < listing.size(); i++) {
        			ftpFiles[i] = listing.get(i);
        		}
        		LOG.info("Files returned from mainframe parser:-");
        		for (FTPFile f : ftpFiles) {
        			LOG.info(String.format("Name: %s, Type: %s",f.getName(),f.getType()));
        		}
        	}
        }
		else {
      // partitioned datasets have a different FTP listing structure
      LOG.info("Dataset is a partitioned dataset, using default FTP list parsing");
      ftpFiles = ftp.listFiles();
        }
		if (!isGDG) {
			for (FTPFile f : ftpFiles) {
				LOG.info(String.format("Name: %s Type: %s",f.getName(), f.getType()));
				if (f.getType() == FTPFile.FILE_TYPE) {
					// only add datasets if default behaviour of partitioned data sets
					// or if it is a sequential data set, only add if the file name matches exactly
					if (!isSequentialDs || isSequentialDs && f.getName().equals(fileName) && !fileName.equals("")) {
						datasets.add(f.getName());
					}
				}
			}
		} else {
			LOG.info("GDG branch. File list:-");
			for (FTPFile f : ftpFiles) {
				LOG.info(String.format("Name: %s Type: %s",f.getName(), f.getType()));
			}
			if (ftpFiles.length > 0 && ftpFiles[ftpFiles.length-1].getType() == FTPFile.FILE_TYPE) {
				// for GDG - add the last file in the collection
				datasets.add(ftpFiles[ftpFiles.length-1].getName());
			}
		}
      }
    } catch(IOException ioe) {
      throw new IOException ("Could not list datasets from " + pdsName + ":"
          + ioe.toString());
    } finally {
      if (ftp != null) {
        closeFTPConnection(ftp);
      }
    }
    return datasets;
  }

  public static FTPClient getFTPConnection(Configuration conf)
      throws IOException {
    FTPClient ftp = null;
    try {
      String username = conf.get(DBConfiguration.USERNAME_PROPERTY);
      String password;
      if (username == null) {
        username = "anonymous";
        password = "";
      }
      else {
        password = DBConfiguration.getPassword((JobConf) conf);
      }

      String connectString = conf.get(DBConfiguration.URL_PROPERTY);
      String server = connectString;
      int port = 0;
      String[] parts = connectString.split(":");
      if (parts.length == 2) {
        server = parts[0];
        try {
          port = Integer.parseInt(parts[1]);
        } catch(NumberFormatException e) {
          LOG.warn("Invalid port number: " + e.toString());
        }
      }

      if (null != mockFTPClient) {
        ftp = mockFTPClient;
      } else {
        ftp = new FTPClient();
      }

      FTPClientConfig config = new FTPClientConfig(FTPClientConfig.SYST_MVS);
      ftp.configure(config);

      if (conf.getBoolean(JobBase.PROPERTY_VERBOSE, false)) {
        ftp.addProtocolCommandListener(new PrintCommandListener(
            new PrintWriter(System.out), true));
      }
      try {
        if (port > 0) {
          ftp.connect(server, port);
        } else {
          ftp.connect(server);
        }
      } catch(IOException ioexp) {
        throw new IOException("Could not connect to server " + server, ioexp);
      }

      int reply = ftp.getReplyCode();
      if (!FTPReply.isPositiveCompletion(reply)) {
        throw new IOException("FTP server " + server
            + " refused connection:" + ftp.getReplyString());
      }
      LOG.info("Connected to " + server + " on " +
          (port>0 ? port : ftp.getDefaultPort()));
      if (!ftp.login(username, password)) {
        ftp.logout();
        throw new IOException("Could not login to server " + server
            + ":" + ftp.getReplyString());
      }
      // set transfer mode
      String transferMode = conf.get(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE);
      if (StringUtils.equalsIgnoreCase(MainframeConfiguration.MAINFRAME_FTP_TRANSFER_MODE_BINARY,transferMode)) {
        LOG.info("Setting FTP transfer mode to binary");
        // ftp.setFileTransferMode(FTP.BINARY_FILE_TYPE) doesn't work for MVS, it throws a syntax error
        ftp.sendCommand("TYPE I");
        // this is IMPORTANT - otherwise it will convert 0x0d0a to 0x0a = dropping bytes
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
      } else {
        LOG.info("Defaulting FTP transfer mode to ascii");
        ftp.setFileTransferMode(FTP.ASCII_FILE_TYPE);
      }
      // Use passive mode as default.
      ftp.enterLocalPassiveMode();
      LOG.info("System type detected: " + ftp.getSystemType());
    } catch(IOException ioe) {
      if (ftp != null && ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch(IOException f) {
          // do nothing
        }
      }
      ftp = null;
      throw ioe;
    }
    return ftp;
  }

  public static boolean closeFTPConnection(FTPClient ftp) {
    boolean success = true;
    try {
      ftp.noop(); // check that control connection is working OK
      ftp.logout();
    } catch(FTPConnectionClosedException e) {
      success = false;
      LOG.warn("Server closed connection: " + e.toString());
    } catch(IOException e) {
      success = false;
      LOG.warn("Server closed connection: " + e.toString());
    } finally {
      if (ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch(IOException f) {
          success = false;
        }
      }
    }
    return success;
  }

  // Used for testing only
  public static void setMockFTPClient(FTPClient FTPClient) {
    mockFTPClient = FTPClient;
  }

}
