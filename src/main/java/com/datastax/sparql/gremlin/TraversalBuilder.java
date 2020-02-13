/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastax.sparql.gremlin;

import com.datastax.sparql.graph.LambdaHelper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;


public class TraversalBuilder {

    public static String PREFIX_KEY_NAME = "GREMLINATOR_PREFIX_KEYNAME";
    public static String PREDICATE_KEY_NAME = "GREMLINATOR_PREDICATE_KEYNAME";

    public static GraphTraversal transform(final Triple triple, boolean invertEdge, Map<Object, UUID> vertexIdToUuid, LambdaHelper lambdas) {
        final Node subject = invertEdge ? triple.getObject() : triple.getSubject();
        final Node object = invertEdge ? triple.getSubject() : triple.getObject();
        final Node predicate = triple.getPredicate();

        Object subjectValue;

        if (subject.isURI()) {
            subjectValue = getUriValueFromVertexNode(subject);
        } else if (subject.isLiteral()) {
            subjectValue = subject.getLiteralValue();
        } else if (subject.isVariable()) {
            subjectValue = subject.getName();
        } else {
            throw new IllegalStateException(String.format("Unexpected subject type: %s", subject));
        }

        GraphTraversal matchTraversal;

        if (subject.isConcrete()) {
            UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
            matchTraversal = __.as(uuid.toString()).hasId(subjectValue);
        } else {
            matchTraversal = __.as(subjectValue.toString());
        }

        Function<String, GraphTraversal> fn = invertEdge ? matchTraversal::in : matchTraversal::out;

        if (predicate.isURI()) {
            final String predicateUri = predicate.getURI();
            final String predicateName = Prefixes.getURIValue(predicateUri);
            final String predicateType = Prefixes.getPrefix(predicateUri);

            switch (predicateType) {
                case "edge":
                    GraphTraversal<?, ?> otherV = fn.apply(predicateName);

                    if (object.isURI()) {
                        return otherV.hasId(getUriValueFromVertexNode(object));
                    } else if (object.isLiteral()) {
                        return otherV.hasId(object.getLiteralValue());
                    } else if (object.isVariable()) {
                        return otherV.as(object.getName());
                    } else {
                        throw new IllegalStateException(String.format("Unexpected object type: %s", object));
                    }
                case "edge-proposition":
                    if (object.isConcrete()) {
                        throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
                    } else {
                        return matchTraversal.outE(predicateName).as(object.getName());
                    }
                case "edge-proposition-subject":  // <eid> eps:edge_label <vid>
                    if (object.isURI()) {
                        if (Prefixes.isValidVertexIdUri(object.getURI())) {
                            String sv = subject.isConcrete()
                                ? vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID()).toString()
                                : subjectValue.toString();
                            return matchTraversal.choose(lambdas.getType())
                                .option("vertex", __.select(sv).outE().inV().hasId(getUriValueFromVertexNode(object)))
                                .option("edge", __.select(sv).inV().hasId(getUriValueFromVertexNode(object)));
                        } else {
                            throw new IllegalStateException(String.format("Unsupported object type for %s - %s", predicate, object));
                        }
                    } else if (object.isLiteral()) {
                        throw new IllegalStateException(String.format("Unsupported object type for %s - %s", predicate, object));
                    } else if (object.isVariable()) {
                        return matchTraversal.inV().as(object.getName());
                    } else {
                        throw new IllegalStateException(String.format("Unsupported object type for %s - %s", predicate, object));
                    }
                case "property":
                    return matchProperty(matchTraversal, predicateName, PropertyType.PROPERTY, object, subjectValue, lambdas);
                case "value":
                    return matchProperty(matchTraversal, predicateName, PropertyType.VALUE, object, subjectValue, lambdas);
                default:
                    throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
            }
        } else if (predicate.isVariable()) {
            String predName = predicate.getName();
            String propStepName = UUID.randomUUID().toString();
            String edgeStepName = UUID.randomUUID().toString();

            if (subject.isURI()) {
                String subjPrefix = Prefixes.getPrefix(subject.getURI());

                UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
                String subjLabel = uuid.toString();

                switch (subjPrefix) {
                    case "vertex-id":
                        if (object.isLiteral()) { // <vid> ?PRED <value>
                            Object objValue = object.getLiteralValue();

                            return __.as(subjLabel).hasId(subjectValue).union(
                                // starting with a vertex
                                __.hasId(objValue).<Object>constant("v:id").as(predName),
                                __.hasLabel(objValue.toString()).<Object>constant("v:label").as(predName),
                                __.properties().as(propStepName).value().is(objValue)
                                    .select(propStepName).key().map(lambdas.v()).as(predName)
                            );
                        } else if (object.isVariable()){ // <vid> ?PRED ?OBJ
                            String objName = object.getName();

                            return __.as(subjLabel).hasId(subjectValue).union(
                                __.id().as(objName).<Object>constant("v:id").as(predName),
                                __.label().as(objName).<Object>constant("v:label").as(predName),
                                __.properties().as(propStepName).key().map(lambdas.v()).as(predName)
                                    .select(propStepName).value().as(objName),
                                __.properties().as(objName).key().map(lambdas.p()).as(predName),
                                __.outE().as(edgeStepName).otherV().as(objName)
                                    .select(edgeStepName).label().map(lambdas.e()).as(predName),
                                __.outE().as(objName).label().map(lambdas.ep()).as(predName)
                            );
                        } else if (object.isURI()) { // <vid> ?PRED <vid>
                            String objValue = getUriValueFromVertexNode(object);

                            return __.as(subjLabel).hasId(subjectValue)
                                .select(subjLabel).outE().as(edgeStepName).otherV().hasId(objValue)
                                .select(edgeStepName).label().map(lambdas.e()).as(predName);
                        }
                    default:
                        throw new IllegalStateException("Unbound predicate with non vertex URI subject is not supported");
                }
            } else if (subject.isVariable()) {
                String subjLabel = subject.getName();

                if (object.isVariable()) {  // ?SUBJ ?PRED ?OBJ
                    String objName = object.getName();

                    return __.as(subjLabel).union(
                        // common for all
                        __.id().as(objName).<Object>constant("v:id").as(predName),
                        __.label().as(objName).<Object>constant("v:label").as(predName),
                        __.properties().as(propStepName).key().map(lambdas.v()).as(predName)
                            .select(propStepName).value().as(objName),
                        __.properties().as(objName).key().map(lambdas.p()).as(predName),

                        // <vid as SUBJ_NAME> <e:eid.label as PRED_NAME> <eid.otherV as OBJ_NAME>
                        __.choose(lambdas.isVertex(),
                            __.outE().as(edgeStepName).otherV().as(objName)
                                .select(edgeStepName).label().map(lambdas.e()).as(predName)),

                        // <vid as SUBJ_LABEL> <ep:eid.label as PRED_NAME> <eid as OBJ_NAME>
                        __.choose(lambdas.isVertex(),
                            __.outE().as(objName).label().map(lambdas.ep()).as(predName)),

                        // <eid as SUBJ_LABEL> <eps:eid.label as PRED_NAME> <eid.otherV as OBJ_NAME>
                        __.choose(lambdas.isVertex(),
                            __.outE().as(subjLabel).otherV().as(objName)
                                .select(subjLabel).label().map(lambdas.eps()).as(predName))
                    );
                } else if (object.isURI()) { // ?SUBJ ?PRED <vid uri>
                    String objValue = getUriValueFromVertexNode(object);

                    return __.as(subjLabel).union(
                        // ?SUBJ is an edge
                        // ?SUBJ is a vertex
                        __.outE().as(edgeStepName).otherV().hasId(objValue)
                            .select(edgeStepName).label().map(lambdas.e()).as(predName),
                        __.outE().as(subjLabel).otherV().hasId(objValue)
                            .select(subjLabel).label().map(lambdas.eps()).as(predName)
                    );
                } else if (object.isLiteral()) { // ?SUBJ ?PRED <string|int|etc>
                    // can be
                    // 1. <vid> <v:vertexPropertyValue> <literal>
                    // 2. <eid> <v:edgePropertyValue> <literal>
                    // 3. <pid> <v:propertyPropertyValue> <literal>
                    Object objValue = object.getLiteralValue();

                    return __.as(subjLabel).union(
                        // starting with a vertex or edge
                        __.hasId(objValue).<Object>constant("v:id").as(predName),
                        __.hasLabel(objValue.toString()).<Object>constant("v:label").as(predName),
                        __.properties().as(propStepName).value().is(objValue)
                            .select(propStepName).key().map(lambdas.v()).as(predName),

                        // starting with an edge
                        __.outE().as(subjLabel).hasId(objValue).<Object>constant("v:id").as(predName),
                        __.outE().as(subjLabel).hasLabel(objValue.toString()).<Object>constant("v:label").as(predName),
                        __.outE().as(subjLabel).properties().as(propStepName).value().is(objValue)
                            .select(propStepName).key().map(lambdas.v()).as(predName));
                } else {
                    throw new IllegalStateException("Unsupported triple " + triple);
                }
            } else {
                throw new IllegalStateException("Unbound predicate with literal subject is not supported " + triple);
            }
        } else {
            throw new IllegalStateException(String.format("Unsupported predicate: %s in %s.", predicate, triple));
        }
    }

    private static String getUriValueFromVertexNode(Node vertexNode) {
        String nodeUri = vertexNode.getURI();

        if (!Prefixes.isValidVertexIdUri(nodeUri)) {
            throw new IllegalStateException("Unsupported Vertex Node URI: " + nodeUri);
        }

        return Prefixes.getURIValue(nodeUri);
    }

    private static GraphTraversal matchProperty(final GraphTraversal traversal, final String propertyName,
                                                      final PropertyType type, final Node object,
                                                      final Object subjectValue, final LambdaHelper lambdas) {
        switch (propertyName) {
            case "id":
                return object.isConcrete()
                    ? traversal.choose(lambdas.isElement()).option(true, __.hasId(object.getLiteralValue()))
                    : traversal.choose(lambdas.isElement()).option(true, __.id().as(object.getName()));
            case "label":
                return object.isConcrete()
                    ? traversal.choose(lambdas.isElement()).option(true, __.hasLabel(object.getLiteralValue().toString()))
                    : traversal.choose(lambdas.isElement()).option(true, __.label().as(object.getName()));
            case "key":
                return object.isConcrete()
                    ? traversal.hasKey(object.getLiteralValue().toString())
                    : traversal.key().as(object.getName());
            case "value":
                return object.isConcrete()
                    ? traversal.hasValue(object.getLiteralValue().toString())
                    : traversal.value().as(object.getName());
            default:
                if (type.equals(PropertyType.PROPERTY)) {
                    return traversal.properties(propertyName).as(object.getName());
                } else {
                    return object.isConcrete()
                        ? traversal.values(propertyName).is(object.getLiteralValue())
                        : traversal.values(propertyName).as(object.getName());
                }
        }
    }
}
