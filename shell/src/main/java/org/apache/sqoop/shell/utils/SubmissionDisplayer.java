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
package org.apache.sqoop.shell.utils;

import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;

import java.text.SimpleDateFormat;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Class used for displaying or printing the submission details
 */
public final class SubmissionDisplayer {

  private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

  /**
   * On job submission, displays the initial job info
   * @param submission
   */
  public static void displayHeader(MSubmission submission) {
    println("@|bold "+ resourceString(Constants.RES_SUBMISSION_SUBMISSION_DETAIL) +"|@");

    print(resourceString(Constants.RES_SUBMISSION_JOB_ID)+": ");
    println(submission.getJobId());

    print(resourceString(Constants.RES_SUBMISSION_SERVER_URL)+": ");
    println(getServerUrl());

    print(resourceString(Constants.RES_SUBMISSION_CREATION_USER) + ": ");
    println(submission.getCreationUser());

    print(resourceString(Constants.RES_SUBMISSION_CREATION_DATE)+": ");
    println(dateFormat.format(submission.getCreationDate()));

    print(resourceString(Constants.RES_SUBMISSION_UPDATE_USER) + ": ");
    println(submission.getLastUpdateUser());

    String externalId = submission.getExternalJobId();
    if(externalId != null) {
      print(resourceString(Constants.RES_SUBMISSION_EXTERNAL_ID)+": ");
      println(externalId);

      String externalLink = submission.getExternalLink();
      if(externalLink != null) {
        println("\t" + externalLink);
      }
    }

    if(isVerbose() && submission.getFromSchema() != null && !submission.getFromSchema().isEmpty() ) {
      print(resourceString(Constants.RES_FROM_SCHEMA)+": ");
      println(submission.getFromSchema());
    }

    if(isVerbose() && submission.getToSchema() != null && !submission.getToSchema().isEmpty() ) {
      print(resourceString(Constants.RES_TO_SCHEMA)+": ");
      println(submission.getToSchema());
    }
  }

  /**
   * Displays the progress of the executing job
   * @param submission
   */
  public static void displayProgress(MSubmission submission) {
    StringBuilder sb = new StringBuilder();
    if(submission.getStatus().isRunning()) {
      sb.append(dateFormat.format(submission.getLastUpdateDate())+": @|green "+submission.getStatus()+ " |@");
      double progress = submission.getProgress();
      sb.append(" - ");
      if(progress == -1) {
        sb.append(resourceString(Constants.RES_SUBMISSION_PROGRESS_NOT_AVAIL));
      } else {
        sb.append(String.format("%.2f %%", progress * 100));
      }
    } else {
      sb.append(dateFormat.format(submission.getLastUpdateDate())+": "+submission.getStatus());
    }

    println(sb.toString());
  }

  /**
   * On successfull or error, method is invoked
   * @param submission
   */
  public static void displayFooter(MSubmission submission) {
    if (submission.getStatus().toString().equals(SubmissionStatus.SUCCEEDED.toString())) {
      println(dateFormat.format(submission.getLastUpdateDate())+": @|green "+submission.getStatus()+ " |@");
      Counters counters = submission.getCounters();
      if (counters != null) {
        println(resourceString(Constants.RES_SUBMISSION_COUNTERS) + ":");
        for (CounterGroup group : counters) {
          print("\t");
          println(group.getName());
          for (Counter counter : group) {
            print("\t\t");
            print(counter.getName());
            print(": ");
            println(counter.getValue());
          }
        }
        println(resourceString(Constants.RES_SUBMISSION_EXECUTED_SUCCESS));
      }
    } else {
      if (submission.getStatus().isFailure()) {
        println(dateFormat.format(submission.getLastUpdateDate())+": @|red "+submission.getStatus()+ " |@");
      } else {
        println(dateFormat.format(submission.getLastUpdateDate())+": "+submission.getStatus());
      }
      // Exception handling
      if (submission.getError().getErrorSummary() != null) {
        print("@|red Exception: |@");
        println(submission.getError().getErrorSummary());

        if (isVerbose() && submission.getError().getErrorDetails() != null) {
          print("@|bold Stack trace: |@");
          println(submission.getError().getErrorDetails());
        }
      }
    }
  }

  public static void displaySubmission(MSubmission submission) {
    if(submission.getStatus().isFailure() || submission.getStatus().equals(SubmissionStatus.SUCCEEDED)) {
      SubmissionDisplayer.displayHeader(submission);
      SubmissionDisplayer.displayFooter(submission);
    } else {
      SubmissionDisplayer.displayHeader(submission);
      SubmissionDisplayer.displayProgress(submission);
    }
  }
}
