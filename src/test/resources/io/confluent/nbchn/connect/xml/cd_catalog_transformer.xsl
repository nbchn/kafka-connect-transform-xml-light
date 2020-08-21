<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
    <xsl:template match="/">
        <simplified_catalog>
            <xsl:for-each select="catalog/cd">
                <simplified_cd>
                    <title><xsl:value-of select="title"/></title>
                    <artist><xsl:value-of select="artist"/></artist>
                </simplified_cd>
            </xsl:for-each>
        </simplified_catalog>
    </xsl:template>
</xsl:stylesheet>