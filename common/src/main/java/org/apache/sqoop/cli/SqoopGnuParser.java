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
package org.apache.sqoop.cli;

import java.util.ListIterator;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

public class SqoopGnuParser extends GnuParser {
  public SqoopGnuParser() {
    super();
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected void processOption(final String arg, final ListIterator iter) throws ParseException {
    boolean hasOption = getOptions().hasOption(arg);

    if (hasOption) {
      super.processOption(arg, iter);
    } else {
      throw new ParseException("Unknown option encountered: " + arg);
    }
  }
}
