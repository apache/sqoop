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
package org.apache.sqoop.model;

import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

/**
 * Metadata object for submission (executed job).
 *
 * Please note that not all properties are persisted in repository at the
 * moment.
 */
public class MSubmission extends MAccountableEntity {

  /**
   * Job id that this submission object belongs.
   *
   * By transitivity of metadata structure you can get also connection and
   * connector ids.
   *
   * This property is required and will be always present.
   */
  private long jobId;

  /**
   * Last known submission status.
   *
   * This property is required and will be always present.
   */
  SubmissionStatus status;

  /**
   * Any valid external id associated with this submission.
   *
   * This property might be missing, but it's value will be recorded in metastore.
   */
  String externalId;

  /**
   * Progress in the job.
   *
   * This is optional property that is not serialized in metastore.
   */
  double progress;

  /**
   * Counters associated with the job if it's already in finished state
   *
   * This is optional property that is not serialized in metastore.
   */
  Counters counters;

  /**
   * Link to external UI if available
   *
   * This is optional property that is not serialized in metastore.
   */
  String externalLink;

  /**
   * Associated exception info with this job (if any).
   *
   * This is optional property that is not serialized in metastore.
   */
  String exceptionInfo;

  /**
   * Associated exception stacktrace with this job (if any).
   *
   * This is optional property that is not serialized in metastore.
   */
  String exceptionStackTrace;

  public MSubmission() {
    status = SubmissionStatus.UNKNOWN;
    progress = -1;
  }

  public MSubmission(long jobId, Date creationDate, SubmissionStatus status) {
    this();
    this.jobId = jobId;
    this.status = status;
  }

  public MSubmission(long jobId) {
    this(jobId, new Date(), SubmissionStatus.BOOTING);
  }

  public MSubmission(long jobId, Date creationDate, SubmissionStatus status,
                     String externalId) {
    this(jobId, creationDate, status);
    this.externalId = externalId;
  }

  public MSubmission(long jobId, Date creationDate, SubmissionStatus status,
                     String externalId, String externalLink, Counters counters){
    this(jobId, creationDate, status, externalId);
    this.externalLink = externalLink;
    this.counters = counters;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public long getJobId() {
    return jobId;
  }

  public void setStatus(SubmissionStatus status) {
    this.status = status;
  }

  public SubmissionStatus getStatus() {
    return status;
  }

  public void setExternalId(String externalId) {
    this.externalId = externalId;
  }

  public String getExternalId() {
    return externalId;
  }

  public void setProgress(double progress) {
    this.progress = progress;
  }

  public double getProgress() {
    return progress;
  }

  public void setCounters(Counters counters) {
    this.counters = counters;
  }

  public Counters getCounters() {
    return counters;
  }

  public void setExternalLink(String externalLink) {
    this.externalLink = externalLink;
  }

  public String getExternalLink() {
    return externalLink;
  }

  public void setExceptionInfo(String exceptionInfo) {
    this.exceptionInfo = exceptionInfo;
  }

  public String getExceptionInfo() {
    return exceptionInfo;
  }

  public void setExceptionStackTrace(String stackTrace) {
    this.exceptionStackTrace = stackTrace;
  }

  public String getExceptionStackTrace() {
    return exceptionStackTrace;
  }

  public void setException(Throwable e) {
    // Exception info
    this.setExceptionInfo(e.toString());

    // Exception stack trace
    StringWriter writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    writer.flush();
    this.setExceptionStackTrace(writer.toString());
  }

  @Override
  public String toString() {
    return "MSubmission{" +
      "jobId=" + jobId +
      ", creationDate=" + getCreationDate() +
      ", lastUpdateDate=" + getLastUpdateDate() +
      ", status=" + status +
      ", externalId='" + externalId + '\'' +
      ", progress=" + progress +
      ", counters=" + counters +
      ", externalLink='" + externalLink + '\'' +
      ", exceptionInfo='" + exceptionInfo + '\'' +
      ", exceptionStackTrace='" + exceptionStackTrace + '\'' +
      '}';
  }

  public static MSubmission UNKNOWN = new MSubmission();
}
