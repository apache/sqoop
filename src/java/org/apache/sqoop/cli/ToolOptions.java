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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.cloudera.sqoop.cli.RelatedOptions;

/**
 * Class that holds several sets of related options, providing a container
 * for all the options associated with a single tool.
 * The order in which sets of related options are added to this tool is
 * preserved in printing and iteration.
 */
public class ToolOptions implements Iterable<RelatedOptions> {

  private List<RelatedOptions> optGroups;

  public ToolOptions() {
    this.optGroups = new ArrayList<RelatedOptions>();
  }

  /**
   * Add a block of related options to the options for this tool.
   * @param opts the set of RelatedOptions to add.
   */
  public void addOptions(RelatedOptions opts) {
    optGroups.add(opts);
  }

  /**
   * Add a block of related options to the options for this tool,
   * if a block has not already been added with the same title.
   * @param opts the set of RelatedOptions to add.
   */
  public void addUniqueOptions(RelatedOptions opts) {
    if (!containsGroup(opts.getTitle())) {
      optGroups.add(opts);
    }
  }

  /**
   * Reports whether this collection of RelatedOptions contains
   * a RelatedOptions with the specified title.
   * @param title the group title to search for
   * @return true if a RelatedOptions with this group title is
   * in the collection.
   */
  public boolean containsGroup(String title) {
    for (RelatedOptions related : this) {
      if (related.getTitle().equals(title)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Provide an iterator over all sets of RelatedOptions.
   * @return an iterator returning each RelatedOptions element.
   */
  public Iterator<RelatedOptions> iterator() {
    return optGroups.iterator();
  }


  /**
   * Flatten the different sets of related options into a single collection
   * of options.
   * @return all options in the ToolOptions as a single set
   */
  public Options merge() {
    Options mergedOpts = new Options();
    int totalOpts = 0;
    for (RelatedOptions relatedOpts : this) {
      for (Object optObj : relatedOpts.getOptions()) {
        Option opt = (Option) optObj;
        mergedOpts.addOption(opt);
        totalOpts++;
      }
    }

    return mergedOpts;
  }

  /**
   * Print the help to the console using a default help formatter.
   */
  public void printHelp() {
    printHelp(new HelpFormatter());
  }

  /**
   * Print the help to the console using the specified help formatter.
   * @param formatter the HelpFormatter to use.
   */
  public void printHelp(HelpFormatter formatter) {
    printHelp(formatter, new PrintWriter(System.out, true));
  }

  /**
   * Print the help to the specified PrintWriter, using the specified
   * help formatter.
   * @param formatter the HelpFormatter to use.
   * @param pw the PrintWriter to emit to.
   */
  public void printHelp(HelpFormatter formatter, PrintWriter pw) {
    boolean first = true;
    for (RelatedOptions optGroup : optGroups) {
      if (!first) {
        pw.println("");
      }
      pw.println(optGroup.getTitle() + ":");
      formatter.printOptions(pw, formatter.getWidth(), optGroup, 0, 4);
      first = false;
    }
  }

  @Override
  public String toString() {
    StringWriter sw = new StringWriter();
    printHelp(new HelpFormatter(), new PrintWriter(sw));
    sw.flush();
    return sw.getBuffer().toString();
  }

}

