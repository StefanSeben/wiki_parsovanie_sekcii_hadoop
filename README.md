# wiki_parsovanie_sekcii_hadoop

### Použitie: ###
Pre parsovanie sekcií sa používa jar súbor WikiSectionParserM.jar, pre indexáciu a vyhľadávanie v indexe WikiTextSearch.jar.

##### Parsovanie sekcií: #####
hadoop jar \<cesta k jar\>/WikiSectionParserM.jar \<hdfs in súbor\> \<hdfs out súbor\> \<počet reduce úloh\>

##### Zindexovanie súborov: #####
java jar \<cesta k jar\>/WikiTextSearch.jar <index priečinok> \<súbor_1\>...\<súbor_n\>

##### Vyhľadávanie: #####
java jar \<cesta k jar\>/WikiTextSearch.jar <index priečinok>
