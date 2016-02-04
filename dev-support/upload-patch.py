#!/usr/bin/env python
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

# This script will take your local git changes and upload them as a patch JIRA and review
# board. This script has been written to support Sqoop workflow but can work for any project
# that uses JIRA and review board.
#
# This tool depends on reviewboard python APIs, please download them
# from here: https://www.reviewboard.org/downloads/rbtools/
#
#
# Future improvement ideas
# * When submitting review request open an editor to let user fill in the details?
# * Add protection against uploading the same file (patch) twice?
# * Migrate all HTTP calls from urllib2 to requests?
import sys, os, re, urllib2, base64, subprocess, tempfile, shutil
import json
import datetime
import ConfigParser
import requests
from optparse import OptionParser
from rbtools.api.client import RBClient

# Resource file location
RC_PATH = os.path.expanduser("~/.upload-patch.rc")

# Default option values
DEFAULT_JIRA_URL = 'https://issues.apache.org/jira'
DEFAULT_JIRA_RB_LABEL = "Review board"
DEFAULT_JIRA_TRANSITION = "Patch Available"
DEFAULT_RB_URL = 'https://reviews.apache.org'
DEFAULT_RB_REPOSITORY = 'sqoop-sqoop2'
DEFAULT_RB_GROUP = 'sqoop'
DEFAULT_JIRA_USER = None
DEFAULT_JIRA_PASSWORD = None
DEFAULT_RB_USER = None
DEFAULT_RB_PASSWORD = None

# Loading resource file that can contain some parameters
if os.path.exists(RC_PATH):
  rc = ConfigParser.RawConfigParser()
  rc.read(RC_PATH)
  # And override faults from the rc file
  DEFAULT_JIRA_USER       = rc.get("jira", "username")
  DEFAULT_JIRA_PASSWORD   = rc.get("jira", "password")
  DEFAULT_RB_USER         = rc.get("reviewboard", "username")
  DEFAULT_RB_PASSWORD     = rc.get("reviewboard", "password")
  print "Loaded JIRA username from resource file: %s" % DEFAULT_JIRA_USER
  print "Loaded Review board username from resource file: %s" % DEFAULT_RB_USER
else:
  print "Resource file %s not found." % RC_PATH

# Options
parser = OptionParser("Usage: %prog [options]")
parser.add_option("--jira",           dest="jira",            help="JIRA number that this patch is for", metavar="SQOOP-1234")
parser.add_option("--jira-url",       dest="jira_url",        default=DEFAULT_JIRA_URL, help="URL to JIRA instance", metavar="http://jira.com/")
parser.add_option("--jira-user",      dest="jira_user",       default=DEFAULT_JIRA_USER, help="JIRA username", metavar="jarcec")
parser.add_option("--jira-transition",dest="jira_transition", default=DEFAULT_JIRA_TRANSITION,help="Name of the transition when uploading patch", metavar="Patch Available")
parser.add_option("--jira-password",  dest="jira_password",   default=DEFAULT_JIRA_PASSWORD, help="JIRA passowrd", metavar="secret")
parser.add_option("--jira-rb-label",  dest="jira_rb_label",   default=DEFAULT_JIRA_RB_LABEL, help="Label to be used in JIRA for the review board link", metavar="Review")
parser.add_option("--rb-url",         dest="rb_url",          default=DEFAULT_RB_URL, help="URL to Review board instance", metavar="http://rb.com/")
parser.add_option("--rb-group",       dest="rb_group",        default=DEFAULT_RB_GROUP, help="Review group for new review entry", metavar="sqoop")
parser.add_option("--rb-repository",  dest="rb_repository",   default=DEFAULT_RB_REPOSITORY, help="Review board's repository", metavar="sqoop2")
parser.add_option("--rb-user",        dest="rb_user",         default=DEFAULT_RB_USER, help="Review board username", metavar="jarcec")
parser.add_option("--rb-password",    dest="rb_password",     default=DEFAULT_RB_PASSWORD, help="Review board passowrd", metavar="secret")
parser.add_option("-v", "--verbose",  dest="verbose",         action="store_true", default=False, help="Print more debug information while execution")

# Execute given command on command line
def execute(cmd, options):
  if options.verbose:
    print "Executing  command: %s" % (cmd)
  return subprocess.call(cmd, shell=True)

# End program execution with given message and return code
def exit(message, ret=1):
  print "FATAL: %s" % message
  sys.exit(ret)

# Convert given number of bytes to human readable one
# Source: http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def human_readable_size(num, suffix='B'):
  for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
    if abs(num) < 1024.0:
      return "%3.1f%s%s" % (num, unit, suffix)
    num /= 1024.0
  return "%.1f%s%s" % (num, 'Yi', suffix)

# Load given file entirely into memory
def get_file_content(filepath):
  f = open(filepath, mode="r")
  diff = f.read()
  f.close()
  return diff

# Geneate request to JIRA instance
def jira_request(url, options, data, headers):
  request = urllib2.Request(url, data, headers)
  if options.verbose:
    print "JIRA Request: URL = %s, Username = %s, data = %s, headers = %s" % (url, options.jira_user, data, str(headers))
  if options.jira_user and options.jira_password:
    base64string = base64.encodestring('%s:%s' % (options.jira_user, options.jira_password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
  return urllib2.urlopen(request)

# Get response from JIRA in form of JSON and parse the JSON for downstream consumption
def jira_json(url, options, data, headers):
  body = jira_request(url, options, data, headers).read()
  if options.verbose:
    print "Response: %s" % body
  return json.loads(body)

# General details of JIRA issue
def jira_get_issue(options):
  url = "%s/rest/api/2/issue/%s" % (options.jira_url, options.jira)
  return jira_json(url, options, None, {})

# Links associated with the JIRA
def jira_get_links(options):
  url = "%s/rest/api/2/issue/%s/remotelink" % (options.jira_url, options.jira)
  return jira_json(url, options, None, {})

# Create new link
def jira_post_links(link_url, title, options):
  url = "%s/rest/api/2/issue/%s/remotelink" % (options.jira_url, options.jira)
  data = '{"object" : {"url" : "%s", "title" : "%s"}}' % (link_url, title)
  jira_request(url, options, data, {"Content-Type" : "application/json"})

# Possible transitions for JIRA
def jira_get_transitions(options):
  url = "%s/rest/api/2/issue/%s/transitions?expand=transititions.fields" % (options.jira_url, options.jira)
  return jira_json(url, options, None, {})

# Transition JIRA to give state
def jira_post_transitions(transitionId, options):
  url = "%s/rest/api/2/issue/%s/transitions" % (options.jira_url, options.jira)
  data = '{"transition" : {"id" : "%s"}}' % transitionId
  jira_request(url, options, data, {"Content-Type" : "application/json"})

# Create new attachement
def jira_post_attachments(f, options):
  url = "%s/rest/api/2/issue/%s/attachments" % (options.jira_url, options.jira)
  files = {'file':open(f)}
  headers = {"X-Atlassian-Token" : "no-check"}
  requests.post(url, files=files, headers=headers, auth=(options.jira_user, options.jira_password)).text

# Parse and validate arguments
(options, args) = parser.parse_args()
if not options.jira:
  exit("Missing argument --jira")

# Main execution
patch = "%s.patch" % options.jira
execute("git diff HEAD > %s" % patch, options)
if not os.path.exists(patch):
  exit("Can't generate patch locally")

# Verify size of the patch
patchSize = os.path.getsize(patch)
if patchSize == 0:
  exit("Generated empty patch, ending gracefully", 0)
else:
  print "Created patch %s (%s)" % (patch, human_readable_size(patchSize))

# Retrive link to review board if it exists already
reviewBoardUrl = None
linksJson = jira_get_links(options)
for link in linksJson:
  if link.get("object").get("title") == options.jira_rb_label:
    reviewBoardUrl = link.get("object").get("url")
    break
if options.verbose:
  if reviewBoardUrl:
    print "Found associated review board: %s" % reviewBoardUrl
  else:
    print "No associated review board entry found"

# Saving details of the JIRA for various use
print "Getting details for JIRA %s" % (options.jira)
jiraDetails = jira_get_issue(options)

# Verify that JIRA is properly marked with versions (otherwise precommit hook would fail)
versions = []
for version in jiraDetails.get("fields").get("versions"):
  versions = versions + [version.get("name")]
for version in jiraDetails.get("fields").get("fixVersions"):
  versions = versions + [version.get("name")]
if not versions:
  exit("Both 'Affected Version(s)' and 'Fix Version(s)' JIRA fields are empty. Please fill one of them with desired version first.")

# Review board handling
rbClient = RBClient(options.rb_url, username=options.rb_user, password=options.rb_password)
rbRoot = rbClient.get_root()

# The RB REST API don't have call to return repository by name, only by ID, so one have to
# manually go through all the repositories and find the one that matches the corrent name.
rbRepoId = -1
for repo in rbRoot.get_repositories(max_results=500):
  if repo.name == options.rb_repository:
    rbRepoId = repo.id
    break
# Verification that we have found required repository
if rbRepoId == -1:
  exit("Did not found repository '%s' on review board" % options.rb_repository)
else:
  if options.verbose:
    print "Review board repository %s has id %s" % (options.rb_repository, rbRepoId)

# If review doesn't exists we need to create one, otherwise we will update existing one
if reviewBoardUrl:
  # For review board REST APIs we need to get just the ID (the number)
  linkSplit = reviewBoardUrl.split('/')
  reviewId = linkSplit[len(linkSplit)-1]
  print "Updating existing review request %s with new patch" % reviewId
  # Review request itself
  reviewRequest = rbRoot.get_review_request(review_request_id=reviewId)
  # Update diff (the patch) and publish the changes
  reviewRequest.get_diffs().upload_diff(get_file_content(patch))
  draft = reviewRequest.get_draft()
  draft.update(public=True)
else:
  print "Creating new review request"
  jiraSummary = jiraDetails.get('fields').get('summary')
  jiraDescription = jiraDetails.get('fields').get('description')
  # Create review request
  reviewRequest = rbRoot.get_review_requests().create(repository=rbRepoId)
  # Attach patch
  reviewRequest.get_diffs().upload_diff(get_file_content(patch))
  # And add details
  draft = reviewRequest.get_draft()
  draft = draft.update(
    summary='%s: %s' % (options.jira, jiraSummary),
    description=jiraDescription,
    target_groups=options.rb_group,
    target_people=options.rb_user,
    bugs_closed=options.jira
  )
  draft.update(public=True)
  linkSplit = draft.links.review_request.href.split('/')
  reviewId = linkSplit[len(linkSplit)-2]
  reviewBoardUrl = "%s/r/%s" % (options.rb_url, reviewId)
  jira_post_links(reviewBoardUrl, options.jira_rb_label, options)
  print "Created new review: %s" % reviewBoardUrl

# Verify state of the JIRA to see if it's in the right state
if jiraDetails.get("fields").get("status").get("name") != options.jira_transition:
  # JIRA REST API needs transition ID and not the human readable name, so we have to translate it first
  jiraTransitions = jira_get_transitions(options)
  transitionId = -1
  for transition in jiraTransitions.get("transitions"):
    if transition.get("to").get("name") == options.jira_transition:
      transitionId = transition.get("id")
  if transitionId == -1:
    exit("Did not find valid transition id for %s" % options.jira_transition)
  else:
    if options.verbose:
      print "Transition id for transition %s is %s" % (options.jira_transition, transitionId)
  # And finally switch to patch available state
  jira_post_transitions(transitionId, options)
  print "Switch JIRA %s to %s state" % (options.jira, options.jira_transition)
else:
  if options.verbose:
    print "JIRA %s is already in %s" % (options.jira, options.jira_transition)

# Upload generated patch to JIRA itself
jira_post_attachments(patch, options)

# And that's it!
print "And we're done!"