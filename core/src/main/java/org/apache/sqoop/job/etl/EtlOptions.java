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
package org.apache.sqoop.job.etl;

import java.util.HashMap;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.CoreError;
import org.apache.sqoop.job.JobConstants;

/**
 * This class retrieves information for job execution from user-input options.
 */
public class EtlOptions implements Options {

  HashMap<String, String> store = new HashMap<String, String>();

  public EtlOptions(SqoopConnector connector) {
    this.connector = connector;
  }

  private SqoopConnector connector;
  public SqoopConnector getConnector() {
    return connector;
  }

  private JobType jobType = null;
  public enum JobType {
    IMPORT,
    EXPORT
  }
  public JobType getJobType() {
    if (jobType != null) {
      return jobType;
    }

    String option = store.get(JobConstants.INPUT_JOB_JOB_TYPE);
    if (option == null || option.equalsIgnoreCase("IMPORT")) {
      jobType = JobType.IMPORT;
    } else if (option.equalsIgnoreCase("EXPORT")) {
      jobType = JobType.EXPORT;
    } else {
      throw new SqoopException(CoreError.CORE_0012, option);
    }
    return jobType;
  }

  private StorageType storageType = null;
  public enum StorageType {
    HDFS
  }
  public StorageType getStorageType() {
    if (storageType != null) {
      return storageType;
    }

    String option = store.get(JobConstants.INPUT_JOB_STORAGE_TYPE);
    if (option == null || option.equalsIgnoreCase("HDFS")) {
      storageType = StorageType.HDFS;
    } else {
      throw new SqoopException(CoreError.CORE_0012, option);
    }
    return storageType;
  }

  private FormatType formatType = null;
  public enum FormatType {
    TEXT,
    SEQUENCE
  }
  public FormatType getFormatType() {
    if (formatType != null) {
      return formatType;
    }

    String option = store.get(JobConstants.INPUT_JOB_FORMAT_TYPE);
    if (option == null || option.equalsIgnoreCase("TEXT")) {
      formatType = FormatType.TEXT;
    } else if (option.equalsIgnoreCase("SEQUENCE")) {
      formatType = FormatType.SEQUENCE;
    } else {
      throw new SqoopException(CoreError.CORE_0012, option);
    }
    return formatType;
  }

  public String getOutputCodec() {
    return store.get(JobConstants.INPUT_JOB_OUTPUT_CODEC);
  }

  private int maxExtractors = -1;
  public int getMaxExtractors() {
    if (maxExtractors != -1) {
      return maxExtractors;
    }

    String option = store.get(JobConstants.INPUT_JOB_MAX_EXTRACTORS);
    if (option != null) {
      maxExtractors = Integer.parseInt(option);
    } else {
      JobType type = getJobType();
      switch (type) {
      case IMPORT:
        maxExtractors = 4;
        break;
      case EXPORT:
        maxExtractors = 1;
        break;
      default:
        throw new SqoopException(CoreError.CORE_0012, type.toString());
      }
    }
    return maxExtractors;
  }

  private int maxLoaders = -1;
  public int getMaxLoaders() {
    if (maxLoaders != -1) {
      return maxLoaders;
    }

    String option = store.get(JobConstants.INPUT_JOB_MAX_LOADERS);
    if (option != null) {
      maxLoaders = Integer.parseInt(option);
    } else {
      JobType type = getJobType();
      switch (type) {
      case IMPORT:
        maxLoaders = 1;
        break;
      case EXPORT:
        maxLoaders = 4;
        break;
      default:
        throw new SqoopException(CoreError.CORE_0012, type.toString());
      }
    }
    return maxLoaders;
  }

  public void setOption(String key, String value) {
    store.put(key, value);
  }

  @Override
  public String getOption(String key) {
    return store.get(key);
  }
}
