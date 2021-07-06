# Introduction
Semantic adapter component performs data transformation from non-semantic to semantic data. In PLATOON architecture context, semantic adapter would be connected to data connectors (ingestion point of data into PLATOON from various sources) to receive non semantic data, would do the data transformation to semantic data and pass on transformed semantic data to upstream components. 

Main functionalities provided by the component

- Data ingestion with provenance for each data connector
- Data transformation rules definition for each source (through [SparqlGenerate](https://ci.mines-stetienne.fr/sparql-generate/))
- Data transformation from non-semantic to semantic data as part of data flow

## Design
Semantic adapter design is based on NiFi that is opensource Apache project for data flow. NiFi’s fundamental design concepts closely relate to the main ideas of Flow Based Programming [fbp] and hence is a natural choice of extension for development of semantic adapter targetted in PLATOON reference architecture.

![Alt text](resources/assets/dfd.png?raw=true "Title")

As a directed data flow graph, built-in processors can be used for various data cleaning/transformation purposes and in the pipeline, `semantic adapter processor` can be used as deemed necessary with respective `sparql-generate` query (sample data and flow is discussed in below sections with sparql-generate query)

### Supported Configuration

Semantic adapter processor;
- Accepts `json` or `csv` or other formats as accepted data input
- A `sparql generate` query with respect to data received in flowfile
- Output format configuration - as of now three output formats are supported `JSON-LD`, `TURTLE` and `RDF/XML` in most cases under PLATOON context, `JSON-LD` will be used
  
![Alt text](resources/assets/config.png?raw=true "Title")

## How to Use

### Build from Source

In this repository you will find source code of NiFi processor that can be built and installed in a running Apache NiFi instance; follow below steps if you are starting with fresh installation

- Download Apache NiFi binary [latest release](https://nifi.apache.org/download.html)
- Run nifi to make sure that it works (requires respective version of Java installed)

    `<nifi-home-directory>/bin/nifi.sh start`
    
- Clone this repository and build
    ```
    # cd <this-repository-directory>
    # mvn clean package
    ```
- Copy processor generated `nar` file `nifi-sparqlgenerate-nar-1.0.nar` to nifi `lib` folder
    ```
    # cp <this-repository-directory>/nifi-sparqlgenerate-nar/target/nifi-sparqlgenerate-nar-1.0.nar <nifi-home-directory>/lib
    
    //restart nifi so that new processor is loaded
    # <nifi-home-directory>/bin/nifi.sh restart
  
    //check logs to see if there are any errors while running the new processor
    # tail -f <nifi-home-directory>/logs/nifi-app.log

    ```
### Semantic Data Transformation Pipeline - Example

A sample flow with data and sparql generate query is made part of this repository and is available under `resources/sample-flow` folder. Considering below array of data being received from source

```
[...
{
    "venue": {
      "latitude": "51.0500000",
      "longitude": "3.7166700"
    },
    "location": {
      "continent": " EU",
      "country": "BE",
      "city": "Paris"
    }
  }
 ...]
```

a sparql-generate query will need to be written to associate semantic ontologies with this source data;

```
base <http://example.com/>
PREFIX iter: <http://w3id.org/sparql-generate/iter/>
PREFIX fn: <http://w3id.org/sparql-generate/fn/>
PREFIX country:<http://loc.example.com/city/>
PREFIX schema: <http://schema.org/>
PREFIX wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX gn: <http://www.geonames.org/ontology#>

GENERATE {
  ?s a schema:City .
  ?s wgs84_pos:lat ?latitude .
  ?s wgs84_pos:long ?longitude .
  ?s gn:countryCode ?countryCode .
}
WHERE {
   BIND(fn:JSONPath(?mainsourcemessage, "$.location.city" ) AS ?city)
   BIND(fn:JSONPath(?mainsourcemessage, "$.venue.latitude" ) AS ?latitude)
   BIND(fn:JSONPath(?mainsourcemessage, "$.venue.longitude" ) AS ?longitude)
   BIND(fn:JSONPath(?mainsourcemessage, "$.location.country" ) AS ?countryCode)
   BIND (URI(CONCAT("http://loc.example.com/city/",?city)) AS ?s)
}
```

and a directed data flow will be created that first splits JSON array to single data elements and then apply sparql-generate query on each to generate semantic data.

![Alt text](resources/assets/flow.png?raw=true "Title")

## Important Notes

- Data flows are directed graph and its possible to use multiple semantic adapter processors in a single data flow depending on each aggregated or raw type data to be transformed
- Scaling of semantic adaptation depends inherently on clustering capacity of apache nifi and hence is able to handle high velocity and volume of data as part of data flow
- Even though semantic adapter based on nifi is suitable for most of the scenarios but for bulk data one time transformations, a separate `semantic adapter bactch` is being developed and can be extended and provided for use for interested partners