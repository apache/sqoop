<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                version="1.0"
                exclude-result-prefixes="exsl">

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
  <a href="http://github.com/cloudera/sqoop">http://github.com/cloudera/sqoop</a>.
  </div>

</xsl:template>
</xsl:stylesheet>
