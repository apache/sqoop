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

public class MainframeConfiguration
{
  public static final String MAINFRAME_INPUT_DATASET_NAME
      = "mapreduce.mainframe.input.dataset.name";

  public static final String MAINFRAME_INPUT_DATASET_TYPE
      = "mapreduce.mainframe.input.dataset.type";
  public static final String MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL
		= "s";
  public static final String MAINFRAME_INPUT_DATASET_TYPE_GDG
	= "g";
  public static final String MAINFRAME_INPUT_DATASET_TYPE_PARTITIONED
	= "p";
  public static final String MAINFRAME_INPUT_DATASET_TAPE = "mainframe.input.dataset.tape";

  public static final String MAINFRAME_FTP_FILE_ENTRY_PARSER_CLASSNAME = "org.apache.sqoop.mapreduce.mainframe.MainframeFTPFileEntryParser";

  public static final String MAINFRAME_FTP_TRANSFER_MODE = "mainframe.ftp.transfermode";

  public static final String MAINFRAME_FTP_TRANSFER_MODE_ASCII = "ascii";

  public static final String MAINFRAME_FTP_TRANSFER_MODE_BINARY = "binary";

  // this is the default buffer size used when doing binary ftp transfers from mainframe
  public static final Integer MAINFRAME_FTP_TRANSFER_BINARY_DEFAULT_BUFFER_SIZE = 32760;

  public static final String MAINFRAME_FTP_TRANSFER_BINARY_BUFFER_SIZE = "mainframe.ftp.buffersize";
}
