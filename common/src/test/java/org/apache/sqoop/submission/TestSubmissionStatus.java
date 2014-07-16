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
package org.apache.sqoop.submission;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

/**
 * Test class for org.apache.sqoop.submission.SubmissionStatus
 */
public class TestSubmissionStatus extends TestCase {

//  /**
//   * unfinished() test
//   */
//  public void testUnfinished() {
//    SubmissionStatus subStatus[] = SubmissionStatus.unfinished();
//    SubmissionStatus subStatusTest[] = new SubmissionStatus[] {
//        SubmissionStatus.RUNNING, SubmissionStatus.BOOTING };
//    List<SubmissionStatus> tempSubmissionStatusList = Arrays.asList(subStatus);
//    for (SubmissionStatus stat : subStatusTest) {
//      assertTrue(tempSubmissionStatusList.contains(stat));
//    }
//  }
//
//  /**
//   * isRunning() test
//   */
//  public void testIsRunning() {
//    assertTrue(SubmissionStatus.RUNNING.isRunning());
//    assertTrue(SubmissionStatus.BOOTING.isRunning());
//    assertFalse(SubmissionStatus.FAILED.isRunning());
//    assertFalse(SubmissionStatus.UNKNOWN.isRunning());
//    assertFalse(SubmissionStatus.FAILURE_ON_SUBMIT.isRunning());
//  }
//
//  /**
//   * isFailure() test
//   */
//  public void testIsFailure() {
//    assertTrue(SubmissionStatus.FAILED.isFailure());
//    assertTrue(SubmissionStatus.UNKNOWN.isFailure());
//    assertTrue(SubmissionStatus.FAILURE_ON_SUBMIT.isFailure());
//    assertFalse(SubmissionStatus.RUNNING.isFailure());
//    assertFalse(SubmissionStatus.BOOTING.isFailure());
//  }
}
