#!/usr/bin/env/python
#
# Copyright 2011 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Generates Apache-style release notes in an HTML file
# for a specific commit range.
#
# Run with '-h' to see usage.

import datetime
import os
import re
import sys

try:
  from xml.etree import ElementTree
except ImportError:
  print "Building release notes is not supported on this platform."
  sys.exit(0)



NUM_ARGS = 6

def print_usage(prgm_name):
  """ Print the usage for this program """
  print "Usage: " + prgm_name + " <target-dir> <git-src> <commit-range> " \
      + "<newversion> <oldversion>"
  print ""
  print "  <target-dir>: Directory where release notes should be written to."
  print "  <git-src>: Root of the git repository to collect info from."
  print "  <commit-range>: What set of commits form this release."
  print "  <newversion>: The version number to print in the release notes."
  print "  <oldversion>: The previous release version number."


def get_log(git_dir, commit_range):
  """ Return the set of lines corresponding to the git log for the specified
      commit range.
  """

  os.chdir(git_dir)
  cmd = "git log --no-color '--pretty=format:%s' '" + commit_range + "'"
  return os.popen(cmd).readlines()


def sanitize_log(in_log):
  """ 'sanitize' the log.
      Some entries do not have a separate subject and body by accident.
      Return a new log that only includes the first sentence of each
      subject. (Note that we also usually have a 'SQOOP-nn.' before this
      sentence.)
  """
  out_log = []
  for line in in_log:
    line = line.strip()
    sentences = line.split(". ")
    if len(sentences) <= 2:
      out_log.append(line) # Unchanged original input.
    else:
      out_log.append(sentences[0] + ". " + sentences[1] + ".")

  return out_log


def get_jira_doc(issue):
  """ Get the XML document from JIRA for a specified issue. """

  xml = os.popen("curl -s 'https://issues.apache.org/jira/si/jira.issueviews:" \
      + "issue-xml/%s/%s.xml?field=key&field=type&field=parent'" % (issue, issue)).read()
  return ElementTree.fromstring(xml)


def get_jira_issue_types(log):
  """ Return a dict from issue-type -> ((issue-name, summary) list) by looking
      up the issues in our JIRA.
  """

  d = {}

  def add_issue(issue, typ, line):
    try:
      d[typ].append((issue, line))
    except KeyError:
      # This issue type hasn't been seen yet. Add a new list.
      d[typ] = [ (issue, line) ]

  jira_reg = r"^(SQOOP-\d+)"
  for line in log:
    matched_line = False
    for m in re.finditer(jira_reg, line, re.M):
      matched_line = True
      jira = m.group(1)
      doc = get_jira_doc(jira)
      issue_type = doc.find('./channel/item/type').text
      # Subtasks use the type of their parent item.
      if issue_type == "Sub-task":
        parent_doc = get_jira_doc(doc.find('./channel/item/parent').text)
        issue_type = parent_doc.find('./channel/item/type').text

      add_issue(jira, issue_type, line)
    if not matched_line and not line.startswith("CLOUDERA-BUILD."):
      # This line did not start with "SQOOP-.."
      # Unless it's a CDH buildfix, add it in as a "Task". 
      add_issue("", "Task", line)

  return d


def get_date():
  """ Return the current month and year formatted as a string. """
  return datetime.date.today().strftime("%B, %Y")


def add_links(summary_line):
  """ Given a line like "SQOOP-40. Do something", add links to the JIRA
      and any appropriate SIPs, and return the line with links.
  """

  initial_jira_reg = r"^(SQOOP-\d+)\. (.*)"

  # Reformat the issue id away from the summary.
  m = re.match(initial_jira_reg, summary_line)
  if m == None:
    # Line in unexpected format. Return as-is.
    return summary_line
  jira = m.group(1)
  text = m.group(2)

  # Add links to JIRA and SIP wiki.

  issue_reg = r"(SQOOP-\d+)"
  issue_subst = r'<a href="https://issues.cloudera.org/browse/\1">\1</a>'

  sip_reg = r"(SIP-\d+)"
  sip_subst = r'<a href="http://wiki.github.com/cloudera/sqoop/\1">\1</a>'

  output = "[" + jira + "] - " + text
  output = re.sub(issue_reg, issue_subst, output)
  output = re.sub(sip_reg, sip_subst, output)

  return output


__user_types = {
    "Bug" : "Bug fixes",
    "Improvement" : "Improvements",
    "New Feature" : "New features",
    "Task" : "Tasks"
}

def user_issue_type(typ):
  """ Return a user-friendly issue type string based on the JIRA issue
      type string.
  """
  global __user_types

  try:
    return __user_types[typ]
  except KeyError:
    # If we don't have a plural-form string set, just use the input.
    return typ



def format_html(newversion, oldversion, log, jira_info):
  """ Creates the HTML representation of the release notes and returns
      it as a string.
  """

  output_lines = []
  output_lines.append("""<html><head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
<title>Sqoop %(newversion)s Release Notes</title>
<style type="Text/css">
h1 {font-family: sans-serif}
h2 {font-family: sans-serif; margin-left: 7mm}
h4 {font-family: sans-serif; margin-left: 7mm}
</style></head>
<body><h1>Release Notes for Sqoop %(newversion)s: %(date)s</h1>

<p>This document lists all Sqoop issues included in version %(newversion)s
not present in the previous release, %(oldversion)s.</p> 
""" % { "newversion" : newversion,
        "oldversion" : oldversion,
        "date"       : get_date() })

  
  # Sort the output list by issue type.
  types = jira_info.keys()
  types.sort()
  for typ in types:
    output_lines.append("<h4>" + user_issue_type(typ) + ":</h4><ul>\n")
    for (issue, summary) in jira_info[typ]:
      output_lines.append("<li>")
      output_lines.append(add_links(summary))
      output_lines.append("</li>\n")
    output_lines.append("</ul>\n")
    
  output_lines.append("</body></html>\n")
  return "".join(output_lines)


def main(argv):
  if len(argv) > 1 and argv[1] == '-h':
    print_usage(argv[0])
    return 0

  if len(argv) < NUM_ARGS:
    print "Missing required argument(s). Try " + argv[0] + " -h"
    return 1

  target_dir = os.path.abspath(os.path.expanduser(argv[1]))
  git_src = os.path.abspath(os.path.expanduser(argv[2]))
  commit_range = argv[3]
  newversion = argv[4]
  oldversion = argv[5]

  log = get_log(git_src, commit_range)
  log = sanitize_log(log)
  jira_info = get_jira_issue_types(log)   
  html = format_html(newversion, oldversion, log, jira_info)

  os.system("mkdir -p \"" + target_dir + "\"")
  handle = open(os.path.join(target_dir, \
      "sqoop-" + newversion + ".releasenotes.html"), "w")
  handle.write(html)
  handle.close()

  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv))


