# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-

import sys
import os

#
# Sphinx configuration
#
# Please refer to the sphinx documentation for details of what and how
# can be configured in this file:
#
# http://sphinx-doc.org/config.html
#

os.environ["GEVENT_NOPATCH"] = "yes"
os.environ["EVENTLET_NOPATCH"] = "yes"
this = os.path.dirname(os.path.abspath(__file__))

# Docs configuration
master_doc = 'index'
project = 'Apache Sqoop'
copyright = '2009-2015 The Apache Software Foundation'

# Build configuration
keep_warnings = True
pygments_style = 'trac'
highlight_language = 'none'

# Output configuration
html_theme = 'sphinxdoc'
html_show_sphinx = False
html_logo = 'sqoop-logo.png'
html_sidebars = {
  '**': ['globaltoc.html'],
}