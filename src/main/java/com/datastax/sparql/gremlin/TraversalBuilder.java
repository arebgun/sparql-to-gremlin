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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;


public class TraversalBuilder {

    public static String PREFIX_KEY_NAME = "GREMLINATOR_PREFIX_KEYNAME";
    public static String PREDICATE_KEY_NAME = "GREMLINATOR_PREDICATE_KEYNAME";

    public static GraphTraversal<?, ?> transform(final Triple triple, boolean invertEdge, Map<Object, UUID> vertexIdToUuid) {
        final Node subject = invertEdge ? triple.getObject() : triple.getSubject();
        final Node object = invertEdge ? triple.getSubject() : triple.getObject();
        final Node predicate = triple.getPredicate();

        Object subjectValue;

        if (subject.isURI()) {
            String subjectUri = subject.getURI();

            if (!Prefixes.isValidVertexIdUri(subjectUri)) {
                throw new IllegalStateException(String.format("Unexpected object URI: %s", subjectUri));
            }

            subjectValue = Prefixes.getURIValue(subjectUri);
        } else if (subject.isLiteral()) {
            subjectValue = subject.getLiteralValue();
        } else if (subject.isVariable()) {
            subjectValue = subject.getName();
        } else {
            throw new IllegalStateException(String.format("Unexpected subject type: %s", subject));
        }

        GraphTraversal<?, ?> matchTraversal;

        if (subject.isConcrete()) {
            UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
            matchTraversal = __.as(uuid.toString()).hasId(subjectValue);
        } else {
            matchTraversal = __.as(subjectValue.toString());
        }

        Function<String, GraphTraversal<?, ?>> fn = invertEdge ? matchTraversal::in : matchTraversal::out;

        if (predicate.isURI()) {
            final String uri = predicate.getURI();
            final String uriValue = Prefixes.getURIValue(uri);
            final String prefix = Prefixes.getPrefix(uri);

            switch (prefix) {
                case "edge":
                    if (object.isURI()) {
                        String objectUri = object.getURI();

                        if (!Prefixes.isValidVertexIdUri(objectUri)) {
                            throw new IllegalStateException(String.format("Unexpected object URI: %s", objectUri));
                        }

                        String objectValue = Prefixes.getURIValue(objectUri);
                        return fn.apply(uriValue).hasId(objectValue);
                    } else if (object.isLiteral()) {
                        return fn.apply(uriValue).hasId(object.getLiteralValue());
                    } else if (object.isVariable()) {
                        return fn.apply(uriValue).as(object.getName());
                    } else {
                        throw new IllegalStateException(String.format("Unexpected object type: %s", object));
                    }
                case "edge-proposition":
                    if (object.isConcrete()) {
                        throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
                    } else {
                        return matchTraversal.outE(uriValue).as(object.getName());
                    }
                case "edge-proposition-subject":
                    if (object.isConcrete()) {
                        throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
                    } else {
                        return matchTraversal.inV().as(object.getName());
                    }
                case "property":
                    return matchProperty(matchTraversal, uriValue, PropertyType.PROPERTY, object);
                case "value":
                    return matchProperty(matchTraversal, uriValue, PropertyType.VALUE, object);
                default:
                    throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
            }
        } else if (predicate.isVariable()) {
            String propStepName = UUID.randomUUID().toString();
            String edgeStepName = UUID.randomUUID().toString();

            if (subject.isURI()) {
                String subjPrefix = Prefixes.getPrefix(subject.getURI());

                switch (subjPrefix) {
                    case "vertex-id":
                        if (object.isLiteral()) { // <vid> ?PRED <value>
                            String predName = predicate.getName();

                            UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
                            String subjLabel = uuid.toString();

                            Object objValue = object.getLiteralValue();

                            return __.as(subjLabel).hasId(subjectValue).union(
                                // starting with a vertex
                                __.match(__.as(subjLabel).hasId(objValue).constant("v:id").as(predName)),
                                __.match(__.as(subjLabel).hasLabel(objValue.toString()).constant("v:label").as(predName)),
                                __.match(__.as(subjLabel).properties().as(propStepName).value().is(objValue),
                                    __.as(propStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("v")).by(T.key).as(predName))
                            );
                        } else if (object.isVariable()){ // <vid> ?PRED ?OBJ
                            String predName = predicate.getName();
                            String objName = object.getName();

                            UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
                            String label = uuid.toString();

                            return __.as(label).hasId(subjectValue).union(
                                __.match(__.as(label).id().as(objName).constant("v:id").as(predName)),
                                __.match(__.as(label).label().as(objName).constant("v:label").as(predName)),
                                __.match(__.as(label).properties().as(propStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("v")).by(T.key).as(predName),
                                    __.as(propStepName).value().as(objName)),
                                __.match(__.as(label).properties().as(objName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("p")).by(T.key).as(predName)),
                                __.match(__.as(label).outE().as(edgeStepName).otherV().as(objName),
                                    __.as(edgeStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("e")).by(T.label).as(predName)),
                                __.match(__.as(label).outE().as(objName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("ep")).by(T.label).as(predName))
                            );
                        } else if (object.isURI()) { // <vid> ?PRED <vid>
                            String predName = predicate.getName();
                            String objectUri = object.getURI();

                            if (!Prefixes.isValidVertexIdUri(objectUri)) {
                                throw new IllegalStateException(String.format("Unexpected object URI: %s", objectUri));
                            }

                            UUID uuid = vertexIdToUuid.computeIfAbsent(subjectValue, v -> UUID.randomUUID());
                            String subjLabel = uuid.toString();
                            String objValue = Prefixes.getURIValue(objectUri);

                            return __.as(subjLabel).hasId(subjectValue).match(
                                __.as(subjLabel).outE().as(edgeStepName).otherV().hasId(objValue),
                                __.as(edgeStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("e")).by(T.label).as(predName)
                            );
                        }
                    default:
                        throw new IllegalStateException("Unbound predicate with non vertex URI subject is not supported");
                }
            } else if (subject.isVariable()) {
                String predName = predicate.getName();
                String subjLabel = subject.getName();

                if (object.isVariable()) {  // ?SUBJ ?PRED ?OBJ
                    String objName = object.getName();

                    return __.as(subjLabel).union(
                        __.match(__.as(subjLabel).id().as(objName).constant("v:id").as(predName)),
                        __.match(__.as(subjLabel).label().as(objName).constant("v:label").as(predName)),
                        __.match(__.as(subjLabel).properties().as(propStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("v")).by(T.key).as(predName),
                            __.as(propStepName).value().as(objName)),
                        __.match(__.as(subjLabel).properties().as(objName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("p")).by(T.key).as(predName)),
                        __.match(__.as(subjLabel).outE().as(edgeStepName).otherV().as(objName),
                            __.as(edgeStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("e")).by(T.label).as(predName)),
                        __.match(__.as(subjLabel).outE().as(objName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("ep")).by(T.label).as(predName)),
                        __.match(__.as(subjLabel).outE().as(objName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("eps")).by(T.label).as(predName))
                    );
                } else if (object.isURI()) { // ?SUBJ ?PRED <vid uri>
                    String objectUri = object.getURI();

                    if (!Prefixes.isValidVertexIdUri(objectUri)) {
                        throw new IllegalStateException(String.format("Unexpected object URI: %s", objectUri));
                    }

                    String objValue = Prefixes.getURIValue(objectUri);

                    return __.as(subjLabel).union(
                        __.match(__.as(subjLabel).outE().as(edgeStepName).otherV().hasId(objValue),
                            __.as(edgeStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("e")).by(T.label).as(predName)),
                        __.match(__.as(subjLabel).outE().as(subjLabel).otherV().hasId(objValue),
                            __.as(subjLabel).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("eps")).by(T.label).as(predName))
                    );
                } else if (object.isLiteral()) { // ?SUBJ ?PRED <string|int|etc>
                    Object objValue = object.getLiteralValue();

                    return __.as(subjLabel).union(
                        // starting with a vertex
                        __.match(__.as(subjLabel).hasId(objValue).constant("v:id").as(predName)),
                        __.match(__.as(subjLabel).hasLabel(objValue.toString()).constant("v:label").as(predName)),
                        __.match(__.as(subjLabel).properties().as(propStepName).value().is(objValue),
                            __.as(propStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("v")).by(T.key).as(predName)),

                        // starting with an edge
                        __.match(__.as(subjLabel).outE().as(subjLabel).hasId(objValue).constant("v:id").as(predName)),
                        __.match(__.as(subjLabel).outE().as(subjLabel).hasLabel(objValue.toString()).constant("v:label").as(predName)),
                        __.match(__.as(subjLabel).outE().as(subjLabel).properties().as(propStepName).value().is(objValue),
                            // for some reason using T.key instead of __.as(propStepName).key() resulted in error
                            __.as(propStepName).project(PREFIX_KEY_NAME, PREDICATE_KEY_NAME).by(__.constant("v")).by(__.as(propStepName).key()).as(predName))
                    );
                } else {
                    throw new IllegalStateException("Unsupported triple " + triple);
                }
            } else {
                throw new IllegalStateException("Unbound predicate with literal subject is not supported " + triple);
            }
        } else {
            throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
        }
    }

    private static GraphTraversal<?, ?> matchProperty(final GraphTraversal<?, ?> traversal, final String propertyName,
                                                      final PropertyType type, final Node object) {
        switch (propertyName) {
            case "id":
                return object.isConcrete()
                    ? traversal.hasId(object.getLiteralValue())
                    : traversal.id().as(object.getName());
            case "label":
                return object.isConcrete()
                    ? traversal.hasLabel(object.getLiteralValue().toString())
                    : traversal.label().as(object.getName());
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
