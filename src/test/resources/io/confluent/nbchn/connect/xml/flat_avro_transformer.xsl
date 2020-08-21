<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
    <xsl:template match="/">
        <simplified_person>
            <nickname><xsl:value-of select="firstname"/></nickname>
        </simplified_person>
    </xsl:template>
</xsl:stylesheet>