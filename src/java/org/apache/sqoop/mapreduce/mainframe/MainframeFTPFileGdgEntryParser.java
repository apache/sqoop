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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.parser.ConfigurableFTPFileEntryParserImpl;

public class MainframeFTPFileGdgEntryParser extends ConfigurableFTPFileEntryParserImpl {
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

  private static final String DEFAULT_DATE_FORMAT = "yyyy/MM/dd HH:mm";
  private static final String HEADER = "Volume Unit ";
  private static String GDG_REGEX = "^\\S+\\s+.*?\\s+(G\\d{4}V\\d{2})$";
  private static final Log LOG = LogFactory.getLog(MainframeFTPFileGdgEntryParser.class.getName());

  public MainframeFTPFileGdgEntryParser() {
    super(GDG_REGEX);
    LOG.info("MainframeFTPFileGdgEntryParser default constructor");
  }

  @Override
  public FTPFile parseFTPEntry(String entry) {
    LOG.info("parseFTPEntry: "+entry);
    if (isFtpListingHeader(entry)) {
      return null;
    }
    if (matches(entry)) {
      String dsName = group(1);
      return createFtpFile(entry,dsName);
    }
    return null;
  }

  protected FTPFile createFtpFile(String entry, String dsName) {
    FTPFile file = new FTPFile();
    file.setRawListing(entry);
    file.setName(dsName);
    file.setType(FTPFile.FILE_TYPE);
    return file;
  }

  protected Boolean isFtpListingHeader(String entry) {
    return entry.startsWith(HEADER);
  }

  @Override
  protected FTPClientConfig getDefaultConfiguration() {
    return new FTPClientConfig(FTPClientConfig.SYST_MVS,
        DEFAULT_DATE_FORMAT, null, null, null, null);
  }

}
