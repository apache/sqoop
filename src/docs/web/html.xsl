<?xml version='1.0'?>

<!-- $FreeBSD: doc/share/xsl/freebsd-html.xsl,v 1.1 2003/01/03 05:06:14 trhodes Exp $ -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'
                xmlns="http://www.w3.org/TR/xhtml1/transitional"
                exclude-result-prefixes="#default">

  <xsl:import href="header.xsl"/>
  <xsl:import href="footer.xsl"/>

  <xsl:template name="body.attributes" />

  <xsl:param name="html.stylesheet" select="'docbook.css'"/>
  <xsl:param name="use.id.as.filename" select="1"/>
  <xsl:param name="generate.legalnotice.link" select="'1'"/>
  <xsl:param name="generate.section.toc.level" select="100"></xsl:param>

  <xsl:param name="toc.section.depth" select="10"/>
  <xsl:param name="section.autolabel" select="1"/>
  <xsl:param name="section.label.includes.component.label" select="1"/>
  <xsl:param name="chunk.section.depth" select="100"></xsl:param>
  <xsl:param name="chunk.first.sections" select="1"></xsl:param>
  <xsl:param name="navig.showtitles" select="1"></xsl:param>

  <xsl:param name="admon.graphics" select="1"></xsl:param>
  <xsl:param name="admon.graphics.extension">.png</xsl:param>
  <xsl:param name="admon.graphics.path">images/</xsl:param>

  <xsl:param name="navig.graphics" select="1"></xsl:param>
  <xsl:param name="navig.graphics.extension">.png</xsl:param>
  <xsl:param name="navig.graphics.path">images/</xsl:param>

  <xsl:param name="header.rule" select="0"></xsl:param>
  <xsl:param name="footer.rule" select="0"></xsl:param>
  <xsl:param name="suppress.header.navigation" select="1"></xsl:param>

  <xsl:param name="generate.index" select="1"></xsl:param>

  <xsl:param name="spacing.paras" select="0"></xsl:param>
  <xsl:param name="html.cleanup" select="1"></xsl:param>

  <xsl:param name="table.borders.with.css" select="1"></xsl:param>
  <!-- xsl:param name="id.warnings" select="1"></xsl:param -->

  <xsl:param name="generate.toc">
appendix  toc,title
article   toc,title,figure,equation
book      toc,title,figure,example,equation
chapter   toc,title
part      toc,title
preface   toc,title
qandadiv  toc
qandaset  toc
reference toc,title
sect1     toc
sect2     toc
sect3     toc
sect4     toc
sect5     toc
section   toc
set       toc,title
   </xsl:param>

</xsl:stylesheet>
