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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counters;

import java.util.Date;

/**
 * Metadata object for submission (executed job).
 *
 * Please note that not all properties are persisted in repository at the
 * moment.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MSubmission extends MAccountableEntity {

  /**
   * Job id that this submission object belongs.
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
   * Any valid external id associated with this job submission.
   * For instance: in the case of MR execution engine, this will refer to the MR job Id
   *
   * This property is optional and might be NULL in case that the job has not
   * yet been submitted to the external system (JobTracker, ResourceManager, ...).
   */
  String externalJobId;

  /**
   * Progress in the job.
   *
   * This property is optional.
   *
   * This property holds the progress of the external process that is executing the
   * Sqoop 2 job. As a result, this property will contain 0 during initialization
   * and submission and will be updated only after the successful submission. Please
   * note that some submission engines might not be able to report the progress at
   * the required granularity and such this property might not be used at all.
   */
  double progress;

  /**
   * Counters associated with the job if it's already in finished state
   *
   * This is property is optional.
   */
  Counters counters;

  /**
   * Link to external UI if available
   *
   * This property is optional.
   */
  String externalLink;

  /**
   * Associated error (exception or failure) with this job (if any).
   *
   * This property is optional.
   */
  SubmissionError error;

  /**
   * Schema for the FROM part of the job submission
   *
   * This property is required, but can be empty.
   */
  Schema fromSchema;

  /**
   * Schema for the TO part of the job submission
   *
   * This property is required, but can be empty.
   */
  Schema toSchema;

  public MSubmission() {
    status = SubmissionStatus.UNKNOWN;
    progress = -1;
  }

  public MSubmission(long jobId, Date creationDate, SubmissionStatus status) {
    this();
    this.jobId = jobId;
    this.status = status;
    setCreationDate(creationDate);
  }

  public MSubmission(long jobId) {
    this(jobId, new Date(), SubmissionStatus.BOOTING);
  }

  public MSubmission(long jobId, Date creationDate, SubmissionStatus status,
                     String externalId) {
    this(jobId, creationDate, status);
    this.externalJobId = externalId;
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

  public void setExternalJobId(String externalJobId) {
    this.externalJobId = externalJobId;
  }

  public String getExternalJobId() {
    return externalJobId;
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

  public void setError(SubmissionError error) {
    this.error = error;
  }

  public SubmissionError getError() {
    if(this.error == null) {
      this.error = new SubmissionError();
    }
    return this.error;
  }

  public Schema getFromSchema() {
    return fromSchema;
  }

  public void setFromSchema(Schema fromSchema) {
    this.fromSchema = fromSchema;
  }

  public Schema getToSchema() {
    return toSchema;
  }

  public void setToSchema(Schema toSchema) {
    this.toSchema = toSchema;
  }

  @Override
  public String toString() {
    return "MSubmission [jobId=" + jobId + ", status=" + status + ", externalId=" + externalJobId
        + ", progress=" + progress + ", counters=" + counters + ", externalLink=" + externalLink
        + ", error=" + error + ", fromSchema=" + fromSchema + ", toSchema=" + toSchema + "]";
  }

  public static MSubmission UNKNOWN = new MSubmission();

}
