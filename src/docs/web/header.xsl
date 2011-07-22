<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                version="1.0"
                exclude-result-prefixes="exsl">

<xsl:import href="breadcrumbs.xsl"/>

<xsl:template name="user.head.content">
</xsl:template>

<xsl:template name="user.header.content">

    <div style="clear:both; margin-bottom: 4px" />
    <div align="center">
      <a href="index.html"><img src="images/home.png"
          alt="Documentation Home" /></a>
    </div>
    <span class="breadcrumbs">
    <xsl:call-template name="breadcrumbs"/>
    </span>

</xsl:template>
</xsl:stylesheet>
