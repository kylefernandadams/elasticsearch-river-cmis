elasticsearch-river-cmis
========================
The over-arching theme of this project is Simplicity and Minimalism. The elasticsearch-river-cmis project leverages ElasticSearch's plugable River service to pull metadata from a Content Management Interoperability Services (CMIS) compliant repository such as Alfresco. Once the metadata has been ETL'd to ElasticSearch we can take advantage of the ElasticSearch Kibana (http://www.elasticsearch.org/overview/kibana/) project to query the ES indices and create a user-driven content analytics dashboard with minimal code and customizations. Can we say path of least resistance?

ElasticSearch River
===================
A river is a pluggable service running within elasticsearch cluster pulling data (or being pushed with data) that is then indexed into the cluster.

More information about ElasticSearch River's can be found in the following documentation:
http://www.elasticsearch.org/guide/en/elasticsearch/rivers/current/

CMIS
====
The Content Management Interoperability Services (CMIS) standard defines a domain model and Web Services and Restful AtomPub bindings that can be used by applications to work with one or more Content Management repositories/systems.
The CMIS interface is designed to be layered on top of existing Content Management systems and their existing programmatic interfaces. It is not intended to prescribe how specific features should be implemented within those CM systems, not to exhaustively expose all of the CM system's capabilities through the CMIS interfaces. Rather, it is intended to define a generic/universal set of capabilities provided by a CM system and a set of services for working with those capabilities.
