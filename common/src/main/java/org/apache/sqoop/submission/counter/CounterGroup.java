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
package org.apache.sqoop.submission.counter;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CounterGroup implements Iterable<Counter> {

  private final String name;
  private Map<String, Counter> counters;

  public CounterGroup(String name) {
    this.name = name;
    this.counters = new HashMap<String, Counter>();
  }

  public String getName() {
    return name;
  }

  public CounterGroup addCounter(Counter counter) {
    counters.put(counter.getName(), counter);
    return this;
  }

  public Counter getCounter(String name) {
    return counters.get(name);
  }

  @Override
  public Iterator<Counter> iterator() {
    return counters.values().iterator();
  }
}
