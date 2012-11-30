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

import org.apache.sqoop.client.core.Environment;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.codehaus.groovy.tools.shell.IO;

import java.text.SimpleDateFormat;

/**
 *
 */
public final class SubmissionDisplayer {

  public static void display(IO io, MSubmission submission) {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss z");
    io.out.println("@|bold Submission details|@");

    io.out.print("Job id: ");
    io.out.println(submission.getJobId());

    io.out.print("Status: ");
    printColoredStatus(io, submission.getStatus());
    io.out.println();

    io.out.print("Creation date: ");
    io.out.println(dateFormat.format(submission.getCreationDate()));

    io.out.print("Last update date: ");
    io.out.println(dateFormat.format(submission.getLastUpdateDate()));

    String externalId = submission.getExternalId();
    if(externalId != null) {
      io.out.print("External Id: ");
      io.out.println(externalId);

      String externalLink = submission.getExternalLink();
      if(externalLink != null) {
        io.out.println("\t" + externalLink);
      }
    }

    if(submission.getStatus().isRunning()) {
      double progress = submission.getProgress();
      io.out.print("Progress: ");
      if(progress == -1) {
        io.out.println("Progress is not available");
      } else {
        io.out.println(String.format("%.2f %%", progress));
      }
    }

    Counters counters = submission.getCounters();
    if(counters != null) {
      io.out.println("Counters:");
      for(CounterGroup group : counters) {
        io.out.print("\t");
        io.out.println(group.getName());
        for(Counter counter : group) {
          io.out.print("\t\t");
          io.out.print(counter.getName());
          io.out.print(": ");
          io.out.println(counter.getValue());
        }
      }
    }

    // Exception handling
    if(submission.getExceptionInfo() != null) {
      io.out.print("@|red Exception: |@");
      io.out.println(submission.getExceptionInfo());

      if(Environment.isVerboose() && submission.getExceptionStackTrace() != null) {
        io.out.print("@|bold Stack trace: |@");
        io.out.println(submission.getExceptionStackTrace());
      }
    }
  }

  public static void printColoredStatus(IO io, SubmissionStatus status) {
    if(status.isRunning()) {
      io.out.print("@|green " + status.toString() + " |@");
    } else if(status.isFailure()) {
      io.out.print("@|red " + status.toString() + " |@");
    } else {
      io.out.print(status.toString());
    }
  }
}
