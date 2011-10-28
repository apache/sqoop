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
package com.cloudera.sqoop.mapreduce.db;


/**
 * Implement DBSplitter over date/time values.
 * Make use of logic from IntegerSplitter, since date/time are just longs
 * in Java.
 *
 * @deprecated use org.apache.sqoop.mapreduce.db.DateSplitter instead.
 * @see org.apache.sqoop.mapreduce.db.DateSplitter
 */
public class DateSplitter extends org.apache.sqoop.mapreduce.db.DateSplitter {

}
