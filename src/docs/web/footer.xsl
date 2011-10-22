<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                version="1.0"
                exclude-result-prefixes="exsl">

<!--
  Copyright 2011 The Apache Software Foundation
 
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->


<xsl:template name="user.footer.content">

  <script type="text/javascript">
     var gaJsHost = (("https:" == document.location.protocol) ? "https://ssl." : "http://www.");
     document.write(unescape("%3Cscript src='" + gaJsHost + "google-analytics.com/ga.js' type='text/javascript'%3E%3C/script%3E"));
  </script>
  <script type="text/javascript">
     try{
        var pageTracker = _gat._getTracker("UA-2275969-4");
        pageTracker._setDomainName(".cloudera.com");
        pageTracker._trackPageview();
     } catch(err) {}
  </script>

  <div class="footer-text">
  <span align="center"><a href="index.html"><img src="images/home.png"
      alt="Documentation Home" /></a></span>
  <br/>
  This document was built from Sqoop source available at
  <a href="http://svn.apache.org/repos/asf/incubator/sqoop/trunk/">http://svn.apache.org/repos/asf/incubator/sqoop/trunk/</a>.
  </div>

</xsl:template>
</xsl:stylesheet>
