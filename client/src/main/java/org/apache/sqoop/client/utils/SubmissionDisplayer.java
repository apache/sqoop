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
package org.apache.sqoop.client.utils;

import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;

import java.text.SimpleDateFormat;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

/**
 *
 */
public final class SubmissionDisplayer {

  public static void display(MSubmission submission) {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
    println("@|bold Submission details|@");

    print("Job id: ");
    println(submission.getJobId());

    print("Status: ");
    printColoredStatus(submission.getStatus());
    println();

    print("Creation date: ");
    println(dateFormat.format(submission.getCreationDate()));

    print("Last update date: ");
    println(dateFormat.format(submission.getLastUpdateDate()));

    String externalId = submission.getExternalId();
    if(externalId != null) {
      print("External Id: ");
      println(externalId);

      String externalLink = submission.getExternalLink();
      if(externalLink != null) {
        println("\t" + externalLink);
      }
    }

    if(submission.getStatus().isRunning()) {
      double progress = submission.getProgress();
      print("Progress: ");
      if(progress == -1) {
        println("Progress is not available");
      } else {
        println(String.format("%.2f %%", progress * 100));
      }
    }

    Counters counters = submission.getCounters();
    if(counters != null) {
      println("Counters:");
      for(CounterGroup group : counters) {
        print("\t");
        println(group.getName());
        for(Counter counter : group) {
          print("\t\t");
          print(counter.getName());
          print(": ");
          println(counter.getValue());
        }
      }
    }

    // Exception handling
    if(submission.getExceptionInfo() != null) {
      print("@|red Exception: |@");
      println(submission.getExceptionInfo());

      if(isVerboose() && submission.getExceptionStackTrace() != null) {
        print("@|bold Stack trace: |@");
        println(submission.getExceptionStackTrace());
      }
    }
  }

  public static void printColoredStatus(SubmissionStatus status) {
    if(status.isRunning()) {
      print("@|green " + status.toString() + " |@");
    } else if(status.isFailure()) {
      print("@|red " + status.toString() + " |@");
    } else {
      print(status.toString());
    }
  }
}
