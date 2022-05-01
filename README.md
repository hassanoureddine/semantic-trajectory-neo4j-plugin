# Neo4j Semantic Trajectories Multiple Views Extraction Procedures

This project implements the multiple views extraction from indoor-outdoor semantic trajectories generated in [this repository](https://github.com/hassanoureddine/offline-semantic-trajectory).

To try this out, simply clone this repository and have a look at the source code.

See [`Neo4j Procedure Template`](https://github.com/neo4j-examples/neo4j-procedure-template) for other template example.

## Building

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file,`target/procedure-template-1.0.0-SNAPSHOT.jar`,
that can be deployed in the `plugin` directory of your Neo4j instance.

## Related articles

* Hassan Noureddine, Cyril Ray, Chritophe Claramunt. [Multiple Views Extraction from Semantic Trajectories](https://www.researchgate.net/publication/360227733_Multiple_Views_Extraction_from_Semantic_Trajectories), 19th International Symposium on Web and Wireless Geographical Information Systems (W2GIS), 2022.
* Hassan Noureddine, Cyril Ray, Chritophe Claramunt. [Multiple Views of Semantic Trajectories in Indoor and Outdoor Spaces](https://www.researchgate.net/publication/355793018_Multiple_Views_of_Semantic_Trajectories_in_Indoor_and_Outdoor_Spaces), 29th International Conference on Advances in Geographic Information Systems (ACM SIGSPATIAL), 2021. 