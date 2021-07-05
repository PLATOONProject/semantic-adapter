/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.crigen.vx.processors.sparqlgenerate;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class SparqlGenerateSemanticAdatperProcessorTest {
    private static final String SPARQL_QUERY = "base <http://example.com/>\n" +
            "PREFIX iter: <http://w3id.org/sparql-generate/iter/>\n" +
            "PREFIX fn: <http://w3id.org/sparql-generate/fn/>\n" +
            "PREFIX country:<http://loc.example.com/city/>\n" +
            "PREFIX schema: <http://schema.org/>\n" +
            "PREFIX wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
            "PREFIX gn: <http://www.geonames.org/ontology#>\n" +
            "\n" +
            "GENERATE {\n" +
            "  ?s a schema:City .\n" +
            "  ?s wgs84_pos:lat ?latitude .\n" +
            "  ?s wgs84_pos:long ?longitude .\n" +
            "  ?s gn:countryCode ?countryCode .\n" +
            "}\n" +
            "WHERE {\n" +
            "   BIND(fn:JSONPath(?mainsourcemessage, \"$.location.city\" ) AS ?city)\n" +
            "   BIND(fn:JSONPath(?mainsourcemessage, \"$.venue.latitude\" ) AS ?latitude)\n" +
            "   BIND(fn:JSONPath(?mainsourcemessage, \"$.venue.longitude\" ) AS ?longitude)\n" +
            "   BIND(fn:JSONPath(?mainsourcemessage, \"$.location.country\" ) AS ?countryCode)\n" +
            "   BIND (URI(CONCAT(\"http://loc.example.com/city/\",?city)) AS ?s)\n" +
            "}";

    private static final String OUTPUT_FORMAT = "JSON-LD";
    private static final String SOURCE_MESSAGE_VARIABLE = "mainsourcemessage";
    private static final String DATA_SOURCE = "{ \"venue\": { \"latitude\": \"51.0500000\", \"longitude\": \"3.7166700\"}, \"location\": {\"continent\": \" EU\",\"country\": \"BE\",\"city\": \"Berlin\"} }";

    private TestRunner testRunner;

    @Before
    public void setup() {

        testRunner = TestRunners.newTestRunner(SparqlGenerateSemanticAdatperProcessor.class);
        testRunner.setProperty(SparqlGenerateSemanticAdatperProcessor.SPARQL_QUERY,SPARQL_QUERY);
        testRunner.setProperty(SparqlGenerateSemanticAdatperProcessor.OUTPUT_FORMAT,OUTPUT_FORMAT);
        testRunner.setProperty(SparqlGenerateSemanticAdatperProcessor.SOURCE_MESSAGE_VARIABLE,SOURCE_MESSAGE_VARIABLE);
        testRunner.setProperty(SparqlGenerateSemanticAdatperProcessor.DATA_SOURCE,DATA_SOURCE);
    }

    @Test
    public void testProcessor() {
        final MockFlowFile flowFile = testRunner.enqueue("{\"London\": { \"country\": \"England\", \"population\": \"8788000\"}}");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(SparqlGenerateSemanticAdatperProcessor.REL_SUCCESS,1);
    }

}
