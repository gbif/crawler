<#ftl output_format="XML">
<?xml version="1.0" encoding="UTF-8"?>
<#macro party tagName agent role="">
  <#if agent??>
  <${tagName}>
    <#if agent.given?? || agent.family?? || agent.displayName??>
    <individualName>
      <#if agent.given??><givenName>${agent.given}</givenName></#if>
      <#if agent.family??>
      <surName>${agent.family}</surName>
      <#elseif agent.literal??>
      <surName>${agent.literal}</surName>
      <#elseif agent.displayName??>
      <surName>${agent.displayName}</surName>
      </#if>
    </individualName>
    </#if>
    <#if agent.primaryOrganization??><organizationName>${agent.primaryOrganization}</organizationName></#if>
    <#if agent.email??><electronicMailAddress>${agent.email}</electronicMailAddress></#if>
    <#if agent.url??><onlineUrl>${agent.url}</onlineUrl></#if>
    <#if role?has_content><role>${role}</role></#if>
  </${tagName}>
  </#if>
</#macro>
<eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"
         xmlns:dc="http://purl.org/dc/terms/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.0.2/eml.xsd"
         packageId="${packageId}" system="http://gbif.org" scope="system"
         xml:lang="${language}">
<dataset>
  <title xml:lang="${language}">${title}</title>
  <#list creators![] as creator>
  <@party tagName="creator" agent=creator />
  </#list>
  <@party tagName="metadataProvider" agent=metadataProvider />
  <#list contributors![] as contributor>
  <@party tagName="associatedParty" agent=contributor role="user" />
  </#list>
  <#if pubDate??><pubDate>${pubDate}</pubDate></#if>
  <language>${language}</language>
  <#if description??>
  <abstract>
    <para>${description}</para>
  </abstract>
  </#if>
  <@party tagName="contact" agent=contact />
  <#if rights??>
  <intellectualRights>
    <para>${rights}</para>
  </intellectualRights>
  </#if>
  <#list alternateIdentifiers![] as identifier>
  <alternateIdentifier>${identifier}</alternateIdentifier>
  </#list>
</dataset>
  <additionalMetadata>
    <metadata>
      <gbif>
          <dateStamp>${dateStamp}</dateStamp>
          <hierarchyLevel>dataset</hierarchyLevel>
      </gbif>
    </metadata>
  </additionalMetadata>
</eml:eml>
