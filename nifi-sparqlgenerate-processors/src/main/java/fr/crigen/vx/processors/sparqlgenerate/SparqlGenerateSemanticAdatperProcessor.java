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

import fr.mines_stetienne.ci.sparql_generate.SPARQLExt;
import fr.mines_stetienne.ci.sparql_generate.engine.PlanFactory;
import fr.mines_stetienne.ci.sparql_generate.engine.RootPlan;
import fr.mines_stetienne.ci.sparql_generate.query.SPARQLExtQuery;
import fr.mines_stetienne.ci.sparql_generate.utils.ContextUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.util.Context;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"semantic, adapter, sparqlgenerate, vx, platoon"})
@CapabilityDescription("This processor transforms non-semantic data to semantic data in realtime. One processor will define exactly one sparql query and expects same type of data that needs to be semantic transformed. Execution of this processor is on local JVM and will be utilizing resource on the same system where NiFi is running. Developed and maintained by Engie Lab CSAI, CRIGEN")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class SparqlGenerateSemanticAdatperProcessor extends AbstractProcessor {

    public static final PropertyDescriptor SPARQL_QUERY = new PropertyDescriptor
            .Builder().name("SPARQL_QUERY")
            .displayName("SparQL Query")
            .description("SparqlGenerate query used to transform input data to semantic data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor
            .Builder().name("OUTPUT_FORMAT")
            .displayName("Output Format")
            .description("RDF Data output format")
            .required(true)
            .allowableValues("TTL","JSON-LD","RDF/XML")
            .defaultValue("TTL")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_MESSAGE_VARIABLE = new PropertyDescriptor
            .Builder().name("SOURCE_MESSAGE_VARIABLE")
            .displayName("Source_Message_Variable")
            .description("The main source message variable is used by the SPARQL Generate Query. For instance in the " +
                    "iteration line of code : ITERATOR iter:CSV(?main_source_message) AS ?message")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_SOURCE = new PropertyDescriptor
            .Builder().name("DATA_SOURCE")
            .displayName("Source Data")
            .description("Leave empty if to be taken from FlowFile")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("If the SparQL Generate process is successful, output will be flown to this relationship")
            .build();

    public static final Relationship REL_FAILED = new Relationship.Builder()
            .name("failed")
            .description("If the SparQL Generate process is failed, error will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SPARQL_QUERY);
        descriptors.add(OUTPUT_FORMAT);
//        descriptors.add(INPUT_FORMAT);
        descriptors.add(DATA_SOURCE);
        descriptors.add(SOURCE_MESSAGE_VARIABLE);

        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<Relationship>();

        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String queryString = context.getProperty(SPARQL_QUERY).getValue();
        String mainSourceMessage = context.getProperty(SOURCE_MESSAGE_VARIABLE).getValue();
        String outputFormat = context.getProperty(OUTPUT_FORMAT).getValue();
        //final String data;
        final ComponentLog logger = getLogger();
        String data = context.getProperty(DATA_SOURCE).getValue();

        try {
            InputStream in = session.read(flowFile);
            data = IOUtils.toString(in);
            in.close();
        } catch (Exception ex) {
            flowFile = session.write(flowFile, (OutputStream out) -> {
                out.write(ex.getMessage().getBytes());
                out.close();
            });
            logger.error(ex.getMessage());
            session.transfer(flowFile, REL_FAILED);
        }

        try {
            // parse query
            String ns = SPARQLExt.NS;
            Syntax SYNTAX = SPARQLExt.SYNTAX;
            SPARQLExtQuery query = (SPARQLExtQuery) QueryFactory.create(queryString, SYNTAX);
            query.setQueryGenerateType();

            if (query.getBaseURI() == null) {
                query.setBaseURI("http://example.org/");
            }

            String variable = mainSourceMessage;
            RootPlan plan = PlanFactory.create(query);
            Model initialModel = ModelFactory.createDefaultModel();
            RDFNode jenaLiteral = initialModel.createLiteral(data);
            QuerySolutionMap initialBinding = new QuerySolutionMap();
            initialBinding.add(variable, jenaLiteral);
            Context sparqlcontext = ContextUtils.build().setInputModel(initialModel).build();
            List<Binding> listBindings = new ArrayList<>();

            BindingHashMap result = new BindingHashMap();
            if (initialBinding != null) {
                Iterator<String> varNames = initialBinding.varNames();
                while (varNames.hasNext()) {
                    String varName = varNames.next();
                    RDFNode node = initialBinding.get(varName);
                    if (node != null) {
                        result.add(Var.alloc(varName), node.asNode());
                    }
                }
            }

            Binding binding = result;
            listBindings.add(binding);

            Model resultModel = plan.execGenerate(listBindings, sparqlcontext);
            OutputStream out = new ByteArrayOutputStream();
            resultModel.write(out, outputFormat);
            String rdfData = out.toString();
            out.close();

            flowFile = session.write(flowFile, (OutputStream outs) -> {
                //logger.info("DATA : " + data);
                logger.info("The RDF file  is" + rdfData);
                logger.info(" " + rdfData.getBytes());
                outs.write(rdfData.getBytes());
                outs.close();
            });
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception ex) {
            flowFile = session.write(flowFile, (OutputStream out) -> {
                out.write(ex.getMessage().getBytes());
                out.close();
            });
            logger.error(ex.getMessage());
            session.transfer(flowFile, REL_FAILED);
        }

    }
}
