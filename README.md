# SobekCM-cmd-LinkedData-URI-updater
A console app that searches a local Apache Jena Fuseki TBD2 triplestore for an exact match for a literal using the LC authorized subject headings dataset and updates the item METS file with the source and authority URIs for the mods:subject elements.

Development plan is to expand the exact match search & update to other metadata such as creators, geographic, and Darwin Core elements.

Note: Does not currently include the disUtility library as it is not ready for public release yet.
