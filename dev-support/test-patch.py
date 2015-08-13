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
#

#
# Pre Commit Hook for running tests and updating JIRA
#
# Original version was copied from FLUME project.
#
import sys, os, re, urllib2, base64, subprocess, tempfile, shutil
import json
import datetime
from optparse import OptionParser
import xml.etree.ElementTree as ET

tmp_dir = None
BASE_JIRA_URL = 'https://issues.apache.org/jira'

# Write output to file
def write_file(filename, content):
  with open(filename, "w") as text_file:
      text_file.write(content)

# Guess branch for given versions
#
# Return None if detects that JIRA belongs to more than one branch
def sqoop_guess_branch(versions):
  branch = None

  for v in versions:
    tmp_branch = None

    if v.startswith("from/to"):
      tmp_branch = "SQOOP-1367"
    elif v.startswith("1.99") or v.startswith("2.0"):
      tmp_branch = "sqoop2"
    else:
      tmp_branch = "trunk"

    if not branch:
      branch = tmp_branch
    else:
      if branch != tmp_branch:
        return None

  return branch

# Open remote URL
def open_url(url):
  print "Opening URL: %s" % (url)
  return urllib2.urlopen(url)

# Verify supported branch
def sqoop_verify_branch(branch):
  return branch in ("sqoop2", "SQOOP-1082", "SQOOP-1367",)

def execute(cmd, log=True):
  if log:
    print "INFO: Executing %s" % (cmd)
  return subprocess.call(cmd, shell=True)

def jenkins_link_for_jira(name, endpoint):
  if "BUILD_URL" in os.environ:
    return "[%s|%s%s]" % (name, os.environ['BUILD_URL'], endpoint)
  else:
    return name

def jenkins_file_link_for_jira(name, file):
  return jenkins_link_for_jira(name, "artifact/patch-process/%s" % file)

def jira_request(result, url, username, password, data, headers):
  request = urllib2.Request(url, data, headers)
  print "INFO: URL = %s, Username = %s, data = %s, headers = %s" % (url, username, data, str(headers))
  if username and password:
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
  return urllib2.urlopen(request)

def jira_get_defect_html(result, defect, username, password):
  url = "%s/browse/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_get_defect(result, defect, username, password):
  url = "%s/rest/api/2/issue/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_color(level):
  if level == ResultItem.INFO:
    return "INFO:"
  elif level == ResultItem.SUCCESS:
    return "{color:green}SUCCESS:{color}"
  elif level == ResultItem.ERROR:
    return "{color:red}ERROR:{color}"
  elif level == ResultItem.FATAL:
    return "{color:red}ERROR:{color}"
  elif level == ResultItem.WARNING:
    return "{color:orange}WARNING:{color}"
  else:
    return level


def jira_generate_comment(result, branch):
  body =  [ "Testing file [%s|%s] against branch %s took %s." % (result.attachment.split('/')[-1] , result.attachment, branch, datetime.datetime.now() - result.start_time) ]
  body += [ "" ]

  if result.overall == "+1":
    body += [ "{color:green}Overall:{color} +1 all checks pass" ]
  else:
    body += [ "{color:red}Overall:{color} -1 due to an error(s), see details below:" ]
  body += [ "" ]

  for item in result._items:
    body += [ "%s %s" % (jira_color(item.level), item.message.replace("\n", "\\n")) ]
    for bullet in item.bullets:
      body += [ "* %s" % bullet ]
    if len(item.bullets) > 0:
      body += [ "\\n" ]

  if "BUILD_URL" in os.environ:
    body += [ "" ]
    body += [ "Console output is available %s." % (jenkins_link_for_jira("here", "console")) ]

  body += [ "" ]
  body += [ "This message is automatically generated." ]
  return "\\n".join(body)

def jira_post_comment(result, defect, branch, username, password):
  url = "%s/rest/api/2/issue/%s/comment" % (BASE_JIRA_URL, defect)

  # Generate body for the comment and save it to a file
  body = jira_generate_comment(result, branch)
  write_file("%s/jira-comment.txt" % output_dir, body.replace("\\n", "\n"))

  # Send the comment to the JIRA
  body = "{\"body\": \"%s\"}" % body
  headers = {'Content-Type' : 'application/json'}
  response = jira_request(result, url, username, password, body, headers)
  body = response.read()
  if response.code != 201:
    msg = """Request for %s failed:
  URL = '%s'
  Code = '%d'
  Comment = '%s'
  Response = '%s'
    """ % (defect, url, response.code, comment, body)
    print "FATAL: %s" % (msg)
    sys.exit(1)

# hack (from hadoop) but REST api doesn't list attachments?
def jira_get_attachment(result, defect, username, password):
  html = jira_get_defect_html(result, defect, username, password)
  pattern = "(/secure/attachment/[0-9]+/(bug)?%s[0-9\-]*((\.|-)v?[0-9]+)?\.(patch|txt|patch\.txt))" % (re.escape(defect))
  matches = []
  for match in re.findall(pattern, html, re.IGNORECASE):
    matches += [ match[0] ]
  if matches:
    matches.sort()
    return  "%s%s" % (BASE_JIRA_URL, matches.pop())
  return None

# Get versions from JIRA JSON object
def json_get_version(json):
  versions = []

  # Load affectedVersion field
  for version in json.get("fields").get("versions"):
    versions = versions + [version.get("name")]

  # Load fixVersion field
  for version in json.get("fields").get("fixVersions"):
    versions = versions + [version.get("name")]

  return versions

def git_checkout(result, branch):
  if not branch:
    result.fatal("Branch wasn't specified nor was correctly guessed")
    return

  if execute("git checkout %s" % (branch)) != 0:
    result.fatal("git checkout %s failed" % branch)
  if execute("git clean -d -f") != 0:
    result.fatal("git clean failed")
  if execute("git reset --hard HEAD") != 0:
    result.fatal("git reset failed")
  if execute("git fetch origin") != 0:
    result.fatal("git fetch failed")
  if execute("git merge --ff-only origin/%s" % (branch)):
    result.fatal("git merge failed")

def git_apply(result, cmd, patch_file, strip, output_dir):
  output_file = "%s/apply.txt" % (output_dir)
  rc = execute("%s -p%s < %s 1>%s 2>&1" % (cmd, strip, patch_file, output_file))
  output = ""
  if os.path.exists(output_file):
    with open(output_file) as fh:
      output = fh.read()
  if rc == 0:
    if output:
      result.warning("Patch applied, but there has been warnings:\n{code}%s{code}\n" % (output))
    else:
      result.success("Patch applied correctly")
  else:
    result.fatal("Failed to apply patch (exit code %d):\n{code}%s{code}\n" % (rc, output))

def static_test(result, patch_file, output_dir):
  output_file = "%s/static-test.txt" % (output_dir)
  rc = execute("grep '^+++.*/test' %s 1>%s 2>&1" % (patch_file, output_file))
  if rc == 0:
    result.success("Patch add/modify test case")
  else:
    result.error("Patch does not add/modify any test case")

def mvn_clean(result, output_dir):
  rc = execute("mvn clean 1>%s/clean.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Clean was successful")
  else:
    result.fatal("Failed to clean project (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "clean.txt")))

def mvn_rat(result, output_dir):
  rc = execute("mvn apache-rat:check 1>%s/rat.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("License check passed")
  else:
    incorrect_files = []
    for path in list(find_all_files(".")):
      file_name = os.path.basename(path)
      if file_name == "rat.txt":
        fd = open(path)
        for line in fd:
          if "!?????" in line:
            matcher = re.search("\!\?\?\?\?\? (.*)$", line)
            if matcher:
              incorrect_files += [ "{{%s}}" % (matcher.groups()[0]) ]
        fd.close()
    result.error("Failed to run license check (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "rat.txt")), set(incorrect_files))

def mvn_install(result, output_dir):
  rc = execute("mvn install -DskipTests 1>%s/install.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Patch compiled")
  else:
    result.fatal("failed to build with patch (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "install.txt")))

def find_all_files(top, fileRegExp=".*", dirRegExp=".*"):
    for root, dirs, files in os.walk(top):
        for f in files:
          if re.search(fileRegExp, f) and re.search(dirRegExp, root):
            yield os.path.join(root, f)

def mvn_test(result, output_dir):
  run_mvn_test("clean test site", "unit", result, output_dir) # Will run cobertura and findbugs as well

def mvn_integration(result, output_dir):
  run_mvn_test("integration-test -pl test", "integration", result, output_dir)

def run_mvn_test(command, test_type, result, output_dir):
  command += " -Pprecommit" # We need special test profile for precommit hook
  test_file_name = "test_%s.txt" % (test_type)
  test_output = "%s/%s" % (output_dir, test_file_name)

  # Execute the test run
  rc = execute("mvn %s 1>%s 2>&1" % (command, test_output))

  # Test run statistics (number of executed/skipped tests)
  executed_tests = 0
  fd = open(test_output)
  for line in fd:
    if "Tests run:" in line:
      matcher = re.search("^Tests run: ([0-9]+), Failures: ([0-9]+), Errors: ([0-9]+), Skipped: ([0-9]+), Time elapsed:", line)
      if matcher:
        executed_tests += int(matcher.group(1))
  fd.close()

  # Based on whether they are
  if rc == 0:
    result.success("All %s tests passed (executed %d tests)" % (test_type ,executed_tests) )
  else:
    failed_tests = []
    for path in list(find_all_files(".")):
      file_name = os.path.basename(path)
      if file_name.startswith("TEST-") and file_name.endswith(".xml"):
        fd = open(path)
        for line in fd:
          if "<failure" in line or "<error" in line:
            matcher = re.search("TEST\-(.*).xml$", file_name)
            if matcher:
               failed_tests += [ "Test {{%s}}" % (matcher.groups()[0]) ]
        fd.close()
    result.error("Some of %s tests failed (%s, executed %d tests)" % (test_type, jenkins_file_link_for_jira("report", test_file_name), executed_tests), set(failed_tests))

def clean_folder(folder):
  for the_file in os.listdir(folder):
      file_path = os.path.join(folder, the_file)
      try:
          if os.path.isfile(file_path):
              os.unlink(file_path)
      except Exception, e:
          print e

# Keep track of actions that we did with their results
class ResultItem(object):
  FATAL = "FATAL"
  ERROR = "ERROR"
  WARNING = "WARNING"
  INFO = "INFO"
  SUCCESS = "SUCCESS"

  def __init__(self, level, message, bullets=[]):
    self.level = level
    self.message = message
    self.bullets = bullets

def cobertura_get_percentage(fd):
  for line in fd:
    if "All Packages" in line:
      matcher = re.search(".*>([0-9]+)%.*>([0-9]+)%.*", line)
      if matcher:
        fd.close()
        return (int(matcher.groups()[0]), int(matcher.groups()[1]))
  fd.close()
  return (-1, -1)

def cobertura_compare(result, output_dir, compare_url):
  # Detailed report file
  report = open("%s/%s" % (output_dir, "cobertura_report.txt"), "w+")
  report.write("Cobertura compare report against %s\n\n" % compare_url)
  # List of all modules for which the test coverage is worst then the compare base
  lowers = []
  # For each report that exists locally
  for path in list(find_all_files(".", "^frame-summary\.html$")):
    package = path.replace("/target/site/cobertura/frame-summary.html", "").replace("./", "")

    remoteIo = None
    try:
      remoteIo = open_url("%s%s" % (compare_url, path))
    except urllib2.HTTPError:
      report.write("Package %s: Base is missing" % (package))
      summary.append("Package {{%p}}: Can't compare test coverage as base is missing." % (package))
      continue

    (localLine, localBranch) = cobertura_get_percentage(open(path))
    (compareLine, compareBranch) = cobertura_get_percentage(remoteIo)

    diffLine = localLine - compareLine
    diffBranch = localBranch - compareBranch

    report.write("Package %s: Line coverage %d (%d -> %d), Branch coverage %d (%d -> %d)\n" % (package, diffLine, compareLine, localLine, diffBranch, compareBranch, localBranch))

    # Cobertura's percentage calculation doesn't seem to be deterministic - it can do +/- 1% even without change a single line. The goal here is to highlight
    # significant troubles when the coverage is diminished substantially, so we're simply workarounding that by ignoring 1% of difference.
    if diffLine < -1 or diffBranch < -1:
      lowers.append("Package {{%s}} has lower test coverage: Line coverage decreased by %d%% (from %d%% to %d%%), Branch coverage decreased by %d%% (from %d%% to %d%%)" % (package, abs(diffLine), compareLine, localLine, abs(diffBranch), compareBranch, localBranch))

  # Add to the JIRA summary report
  if len(lowers) == 0:
    result.success("Test coverage did not decreased (%s)" % jenkins_file_link_for_jira("report", "cobertura_report.txt"))
  else:
    result.warning("Test coverage has decreased (%s)" % jenkins_file_link_for_jira("report", "cobertura_report.txt"), lowers)

  # Clean up
  report.close()

def findbugs_get_bugs(fd):
  root = ET.parse(fd).getroot()
  bugs = {}
  for file in root.findall("file"):
    classname = file.attrib["classname"]
    errors = len(list(file))
    bugs[classname] = errors
  fd.close()
  return bugs

def findbugs_compare(result, output_dir, compare_url):
  # Detailed report file
  report = open("%s/%s" % (output_dir, "findbugs_report.txt"), "w+")
  report.write("Findbugs compare report against %s\n\n" % compare_url)

  # Lines to be added to summary on JIRA
  summary = []

  # For each report that exists locally
  for path in list(find_all_files(".", "^findbugs\.xml$")):
    package = path.replace("/target/findbugs.xml", "").replace("./", "")

    remoteIo = None
    try:
      remoteIo = open_url("%s%s" % (compare_url, path))
    except urllib2.HTTPError:
      report.write("Package %s: Base is missing" % (package))
      summary.append("Package {{%p}}: Can't compare classes as base is missing." % (package))
      continue

    local = findbugs_get_bugs(open(path))
    remote = findbugs_get_bugs(remoteIo)
    report.write("Processing package %s:\n" % (package))

    # Identify the differences for each class
    for classname, errors in local.iteritems():
      report.write("* Class %s has %s errors.\n" % (classname, errors));
      if classname in remote:
        # This particular class is also known to our compare class
        remoteErrors = remote[classname]
        if(int(errors) > int(remoteErrors)):
          summary.append("Package {{%s}}: Class {{%s}} increased number of findbugs warnings to %s (from %s)" % (package, classname, errors, remoteErrors))
      else:
        # This classes errors are completely new to us
        summary.append("Package {{%s}}: Class {{%s}} introduced %s completely new findbugs warnings." % (package, classname, errors))

    # Spacing between each package
    report.write("\n")

  # Add to JIRA summary
  if len(summary) == 0:
    result.success("No new findbugs warnings (%s)" % jenkins_file_link_for_jira("report", "findbugs_report.txt"))
  else:
    result.warning("New findbugs warnings (%s)" % jenkins_file_link_for_jira("report", "findbugs_report.txt"), summary)

  # Finish detailed report
  report.close()

class Result(object):
  def __init__(self):
    self._items = []
    self.overall = "+1"
    self.exit_handler = None
    self.attachment = "Not Found"
    self.start_time = datetime.datetime.now()
  def error(self, msg, bullets=[]):
    self.overall = "-1"
    self._items.append(ResultItem(ResultItem.ERROR, msg, bullets))
  def info(self, msg, bullets=[]):
    self._items.append(ResultItem(ResultItem.INFO, msg, bullets))
  def success(self, msg, bullets=[]):
    self._items.append(ResultItem(ResultItem.SUCCESS, msg, bullets))
  def warning(self, msg, bullets=[]):
    self._items.append(ResultItem(ResultItem.WARNING, msg, bullets))
  def fatal(self, msg, bullets=[]):
    self.overall = "-1"
    self._items.append(ResultItem(ResultItem.FATAL, msg, bullets))
    self.exit_handler()
    self.exit()
  def exit(self):
    sys.exit(0)

usage = "usage: %prog [options]"
parser = OptionParser(usage)
parser.add_option("--branch", dest="branch",
                  help="Local git branch to test against", metavar="trunk")
parser.add_option("--defect", dest="defect",
                  help="Defect name", metavar="FLUME-1787")
parser.add_option("--file", dest="filename",
                  help="Test patch file", metavar="FILE")
parser.add_option("--run-tests", dest="run_tests",
                  help="Run Tests", action="store_true")
parser.add_option("--run-integration-tests", dest="run_integration_tests",
                  help="Run Integration Tests", action="store_true")
parser.add_option("--username", dest="username",
                  help="JIRA Username", metavar="USERNAME", default="flumeqa")
parser.add_option("--output", dest="output_dir",
                  help="Directory to write output", metavar="DIRECTORY")
parser.add_option("--post-results", dest="post_results",
                  help="Post results to JIRA (only works in defect mode)", action="store_true")
parser.add_option("--password", dest="password",
                  help="JIRA Password", metavar="PASSWORD")
parser.add_option("--patch-command", dest="patch_cmd", default="git apply",
                  help="Patch command such as `git apply' or `patch'", metavar="COMMAND")
parser.add_option("-p", "--strip", dest="strip", default="1",
                  help="Remove <n> leading slashes from diff paths", metavar="N")
parser.add_option("--cobertura", dest="cobertura",
                  help="HTTP URL with past cobertura results that we should compare against")
parser.add_option("--findbugs", dest="findbugs",
                  help="HTTP URL with past findbugs results that we should compare against")

(options, args) = parser.parse_args()
if not (options.defect or options.filename):
  print "FATAL: Either --defect or --file is required."
  sys.exit(1)

if options.defect and options.filename:
  print "FATAL: Both --defect and --file cannot be specified."
  sys.exit(1)

if options.post_results and not options.password:
  print "FATAL: --post-results requires --password"
  sys.exit(1)

branch = options.branch
output_dir = options.output_dir
defect = options.defect
username = options.username
password = options.password
run_tests = options.run_tests
run_integration_tests = options.run_integration_tests
post_results = options.post_results
strip = options.strip
patch_cmd = options.patch_cmd
result = Result()

if os.path.isdir(options.output_dir):
  clean_folder(options.output_dir)

# Default exit handler in case that we do not want to submit results to JIRA
def log_and_exit():
  # Write down comment generated for jira (won't be posted)
  write_file("%s/jira-comment.txt" % output_dir, jira_generate_comment(result, branch).replace("\\n", "\n"))

  for item in result._items:
    print "%s: %s" % (item.level, item.message)
    for bullet in item.bullets:
      print "* %s" % bullet

result.exit_handler = log_and_exit

if post_results:
  def post_jira_comment_and_exit():
    jira_post_comment(result, defect, branch, username, password)
    result.exit()
  result.exit_handler = post_jira_comment_and_exit

if not output_dir:
  tmp_dir = tempfile.mkdtemp()
  output_dir = tmp_dir

if output_dir.endswith("/"):
  output_dir = output_dir[:-1]

if options.output_dir and not os.path.isdir(options.output_dir):
  os.makedirs(options.output_dir)

# If defect parameter is specified let's download the latest attachment
if defect:
  print "Defect: %s" % defect
  jira_json = jira_get_defect(result, defect, username, password)
  json = json.loads(jira_json)

  # JIRA must be in Patch Available state
  if '"Patch Available"' not in jira_json:
    print "ERROR: Defect %s not in patch available state" % (defect)
    sys.exit(1)

  # If branch is not specified, let's try to guess it from JIRA details
  if not branch:
    versions = json_get_version(json)
    branch = sqoop_guess_branch(versions)
    if not branch:
      print "ERROR: Can't guess branch name from %s" % (versions)
      sys.exit(1)

  attachment = jira_get_attachment(result, defect, username, password)
  if not attachment:
    print "ERROR: No attachments found for %s" % (defect)
    sys.exit(1)

  result.attachment = attachment

  patch_contents = jira_request(result, result.attachment, username, password, None, {}).read()
  patch_file = "%s/%s.patch" % (output_dir, defect)

  with open(patch_file, 'a') as fh:
    fh.write(patch_contents)
elif options.filename:
  patch_file = options.filename
else:
  raise Exception("Not reachable")

# Verify that we are on supported branch
if not sqoop_verify_branch(branch):
  print "ERROR: Unsupported branch %s" % (branch)
  sys.exit(1)

mvn_clean(result, output_dir)
git_checkout(result, branch)
git_apply(result, patch_cmd, patch_file, strip, output_dir)
static_test(result, patch_file, output_dir)
mvn_rat(result, output_dir)
mvn_install(result, output_dir)
# Unit tests are conditional
if run_tests:
  mvn_test(result, output_dir)
  # Cobertura report is conditional
  if options.cobertura:
    cobertura_compare(result, output_dir, options.cobertura)
  # Findbugs report is also conditional
  if options.findbugs:
    findbugs_compare(result, output_dir, options.findbugs)
else:
  result.info("Unit tests were skipped, please add --run-tests")
# Integration tests are conditional
if run_integration_tests:
  mvn_integration(result, output_dir)
else:
  result.info("Integration tests were skipped, please add --run-integration-tests")

result.exit_handler()
