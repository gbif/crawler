<request xmlns="http://digir.net/schema/protocol/2003/1.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xmlns:dwc="http://digir.net/schema/conceptual/darwin/2003/1.0"
         xsi:schemaLocation="http://digir.net/schema/protocol/2003/1.0 http://digir.sourceforge.net/schema/protocol/2003/1.0/digir.xsd http://digir.net/schema/conceptual/darwin/2003/1.0 ${schemaLocation}">
  <header>
    <version>1.0.0</version>
    <sendTime>${.now?iso_local}</sendTime>
    <source>GBIF Crawler</source>
    <destination resource="${resource}">${destination}</destination>
    <type>search</type>
  </header>
  <search>
    <filter>
      <#if lower?? && upper??>
      <and>
        </#if>
        <#if lower??>
        <greaterThanOrEquals>
          <dwc:ScientificName>${lower}</dwc:ScientificName>
        </greaterThanOrEquals>
        </#if>
        <#if upper??>
        <lessThan>
          <dwc:ScientificName>${upper}</dwc:ScientificName>
        </lessThan>
        </#if>
      <#if lower?? && upper??>
      </and>
      </#if>
    </filter>
    <records limit="${maxResults}" start="${startAt}">
      <structure schemaLocation="${recordSchemaLocation}"/>
    </records>
    <count>false</count>
  </search>
</request>
