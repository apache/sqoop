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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * Options parser that follows Sqoop's specific options-parsing needs.
 * <ul>
 *   <li>Option values may contain the '&quot;' character as the first
 *   or last character in the value.</li>
 *   <li>The argument '--' must be preserved in the returned (unparsed)
 *   argument list.</li>
 * </ul>
 */
public class SqoopParser extends GnuParser {

  public static final Log LOG = LogFactory.getLog(SqoopParser.class.getName());

  // We need a handle to Option.addValueForProcessing(String).
  // Since Hadoop will load this in a different classloader than
  // this Sqoop class, we cannot see their package-specific methods.
  // So we just call it by reflection. As long as we're at it, this
  // allows us to also put SqoopParser in its own package.
  private static java.lang.reflect.Method addValForProcessing;

  static {
    try {
      addValForProcessing = Option.class.getDeclaredMethod(
          "addValueForProcessing", String.class);
      addValForProcessing.setAccessible(true);
    } catch (NoSuchMethodException nsme) {
      LOG.error("Could not load required method of Parser: "
          + StringUtils.stringifyException(nsme));
      addValForProcessing = null;
    }
  }

  @Override
  /**
   * Processes arguments to options but only strips matched quotes.
   */
  public void processArgs(Option opt, ListIterator iter)
      throws ParseException {
    // Loop until an option is found.
    while (iter.hasNext()) {
      String str = (String) iter.next();

      if (getOptions().hasOption(str) && str.startsWith("-")) {
        // found an Option, not an argument.
        iter.previous();
        break;
      }

      // Otherwise, this is a value.
      try {
        // Note that we only strip matched quotes here.
        addValForProcessing.invoke(opt, stripMatchedQuotes(str));
      } catch (IllegalAccessException iae) {
        throw new RuntimeException(iae);
      } catch (java.lang.reflect.InvocationTargetException ite) {
        // Any runtime exception thrown within addValForProcessing()
        // will be wrapped in an InvocationTargetException.
        iter.previous();
        break;
      } catch (RuntimeException re) {
        iter.previous();
        break;
      }
    }

    if (opt.getValues() == null && !opt.hasOptionalArg()) {
      throw new MissingArgumentException(opt);
    }
  }

  /**
   * Util.stripLeadingAndTrailingQuotes() will strip a '&quot;'
   * character from either or both sides of a string. We only
   * strip the matched pair.
   */
  private String stripMatchedQuotes(String in) {
    if (null == in || "\"".equals(in)) {
      return in; // single quote stays as-is.
    } else if (in.startsWith("\"") && in.endsWith("\"")) {
      // Strip this pair of matched quotes.
      return in.substring(1, in.length() - 1);
    } else {
      // return as-is.
      return in;
    }
  }
}

