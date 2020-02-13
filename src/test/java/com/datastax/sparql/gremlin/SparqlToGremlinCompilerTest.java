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

import com.datastax.sparql.graph.GraphFactory;
import com.datastax.sparql.graph.LambdaHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.sparql.ARQConstants;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.sparql.gremlin.SparqlToGremlinCompiler.OPTIONAL_DEFAULT_RESULT;
import static com.datastax.sparql.gremlin.SparqlToGremlinCompiler.compile;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SparqlToGremlinCompilerTest {

    private Graph modern, crew, gotg;
    private GraphTraversalSource mg, cg, gg;
    private GraphTraversalSource mc, cc;

    @Before
    public void setUp() {
        gotg = GraphFactory.createGraphOfTheGods();
        gg = gotg.traversal();
    }

    @Test
    public void testGotgBoundVertexSubject() {
        String query = "\n" +
            "SELECT ?PRED ?OTHER " +
            "WHERE { " +
                "vid:4 ?PRED ?OTHER . " +
            "}";

        LambdaHelper lambdas = new LambdaHelper.NativeJavaLambdas();

        GraphTraversal expected = gg.V().match(
            __.as("SUBJ").hasId(4).union(
                __.id().as("OTHER").<Object>constant("v:id").as("PRED"),
                __.label().as("OTHER").<Object>constant("v:label").as("PRED"),
                __.properties().as("RANDOM_UUID_PROPS").key().map(lambdas.v()).as("PRED")
                    .select("RANDOM_UUID_PROPS").value().as("OTHER"),
                __.properties().as("OTHER").key().map(lambdas.p()).as("PRED"),
                __.outE().as("RANDOM_UUID_EDGE").otherV().as("OTHER")
                    .select("RANDOM_UUID_EDGE").label().map(lambdas.e()).as("PRED"),
                __.outE().as("OTHER").label().map(lambdas.ep()).as("PRED")))
            .select("PRED", "OTHER");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotg_filterOnUris() {
        String query =
            "SELECT ?FOE ?BATTLE ?HERO ?BATTLE_TIME ?FOE_NAME ?HERO_NAME ?BATTLE_PLACE\n" +
            "WHERE {\n" +
            "  ?BATTLE eps:battled ?FOE .\n" +
            "  ?HERO ep:battled ?BATTLE .\n" +
            "  ?BATTLE v:time ?BATTLE_TIME .\n" +
            "  ?FOE v:name ?FOE_NAME .\n" +
            "  ?HERO v:name ?HERO_NAME .\n" +
            "  ?BATTLE v:place ?BATTLE_PLACE .\n" +
            "  FILTER (?FOE = vid:9)\n" +
            "}";

        GraphTraversal actual = compile(gotg, query);
        List resultActual = actual.toList();

        assertEquals(1, resultActual.size());

        Object bindingSet = resultActual.get(0);
        assertTrue(bindingSet instanceof Map);

        Map<String, Object> result = (Map<String, Object>) bindingSet;

        Object vertexBinding = result.get("FOE");
        assertTrue(vertexBinding instanceof Vertex);
        assertEquals(9, ((Vertex) vertexBinding).id());

        Object edgeBinding = result.get("BATTLE");
        assertTrue(edgeBinding instanceof Edge);
        assertEquals(110, ((Edge) edgeBinding).id());

        vertexBinding = result.get("HERO");
        assertTrue(vertexBinding instanceof Vertex);
        assertEquals(6, ((Vertex) vertexBinding).id());

        assertEquals(1, result.get("BATTLE_TIME"));
        assertEquals("nemean", result.get("FOE_NAME"));
        assertEquals("hercules", result.get("HERO_NAME"));
        assertEquals("38.1 23.7", result.get("BATTLE_PLACE"));
    }

    @Test
    public void testGotg_UnboundSubject_EpsPredicate_UriObject() {
        String query =
            "SELECT ?BATTLE " +
                "WHERE { " +
                "  ?BATTLE eps:battled vid:9 . " +
                "}";

        GraphTraversal actual = compile(gotg, query);
        List resultActual = actual.toList();

        assertEquals(1, resultActual.size());
        Object battleBinding = resultActual.get(0);
        assertTrue(battleBinding instanceof Vertex);
        assertEquals(6, ((Vertex) battleBinding).id());
    }

    @Test
    public void testGotg_EdgeToUriObjectViaEp() {
        String query =
            "SELECT ?BATTLE ?HERO ?BATTLE_TIME ?HERO_NAME ?BATTLE_PLACE " +
            "WHERE { " +
            "  ?BATTLE eps:battled <http://northwind.com/model/vertex-id#9> . " +
            "  ?HERO ep:battled ?BATTLE . " +
            "  ?BATTLE v:time ?BATTLE_TIME . " +
            "  ?HERO v:name ?HERO_NAME . " +
            "  ?BATTLE v:place ?BATTLE_PLACE . " +
            "}";

        GraphTraversal actual = compile(gotg, query);
        List resultActual = actual.toList();

        assertEquals(1, resultActual.size());

        Object bindingSet = resultActual.get(0);
        assertTrue(bindingSet instanceof Map);

        Map<String, Object> result = (Map<String, Object>) bindingSet;

        Object edgeBinding = result.get("BATTLE");
        assertTrue(edgeBinding instanceof Edge);
        assertEquals(110, ((Edge) edgeBinding).id());

        Object vertexBinding = result.get("HERO");
        assertTrue(vertexBinding instanceof Vertex);
        assertEquals(6, ((Vertex) vertexBinding).id());

        assertEquals(1, result.get("BATTLE_TIME"));
        assertEquals("hercules", result.get("HERO_NAME"));
        assertEquals("38.1 23.7", result.get("BATTLE_PLACE"));
    }

    @Test
    public void testGotg_UnboundPredicateIndirect() {
        String query = "SELECT ?X ?PRED ?Y WHERE { ?X ?PRED ?Y . ?Y v:label 'battled'}";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String ,Object>> resultActual = actual.toList();
        assertEquals(3, resultActual.size());
        resultActual.forEach(b -> assertEquals("ep:battled", b.get("PRED")));
    }

    @Test
    public void testGotg_UnboundPredicateIndirect2() {
        String query = "SELECT ?X ?PRED ?Y WHERE { ?X ?PRED ?Y . ?Y v:id 112}";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String ,Object>> resultActual = actual.toList();
        assertEquals(1, resultActual.size());
        resultActual.forEach(b -> assertEquals("ep:battled", b.get("PRED")));
    }

    @Test
    public void testGotgFindPredicatesAndValuesConnectingToE() {
        String query = "SELECT ?BATTLE ?PRED ?VALUE WHERE { vid:6 ep:battled ?BATTLE . ?BATTLE ?PRED ?VALUE . }";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();

        int expectedBattleBindingsNum = 3;
        int expectedBattleEdgePropsNum = 6;  // id, label, time, place, p[time], p[place]
        int expectedNum = expectedBattleBindingsNum * expectedBattleEdgePropsNum;
        assertEquals(expectedNum, resultActual.size());
    }

    @Test
    public void testGotg_FullyUnboundSentence() {
        String query = "SELECT DISTINCT ?PRED WHERE { ?X ?PRED ?Y . } LIMIT 5";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();
        System.out.println(resultActual);
    }

    @Test
    public void testGotg_VertexUriSubject_UnboundPredicate_VertexUriObject() {
        String query = "SELECT DISTINCT ?PRED WHERE { vid:4 ?PRED vid:2 . }";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();

        assertEquals(resultActual.size(), 1);
        assertEquals("e:lives", resultActual.get(0));
    }

    @Test
    public void testGotgVertexUriSubjectUnboundPredicateBoundLiteralObjectVertexLabelProperty() {
        String query = "SELECT DISTINCT ?PRED WHERE { vid:2 ?PRED \"location\" . }";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();

        assertEquals(resultActual.size(), 1);
        assertEquals(resultActual.get(0), "v:label");
    }

    @Test
    public void testGotgVertexUriSubjectUnboundPredicateBoundLiteralObjectVertexStringProperty() {
        String query = "SELECT DISTINCT ?PRED WHERE { vid:2 ?PRED \"sky\" . }";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();

        assertEquals(1, resultActual.size());
        assertEquals(resultActual.get(0), "v:name");
    }

    @Test
    public void testGotgVertexUriSubjectUnboundPredicateBoundLiteralObjectVertexIntProperty() {
        String query = "SELECT DISTINCT ?PRED WHERE { vid:1 ?PRED 10000 . }";
        GraphTraversal actual = compile(gotg, query);

        List resultActual = actual.toList();

        assertEquals(resultActual.size(), 1);
        assertEquals("v:age", resultActual.get(0));
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectVertexLabelProperty() {
        String objValue = "god";
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED \"" + objValue + "\" . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 3);

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals(result.get("PRED"),"v:label");
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectVertexStringProperty() {
        String objValue = "sky";
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED \"" + objValue + "\" . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 1);

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals("v:name", result.get("PRED"));
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectVertexIntProperty() {
        int objValue = 30;
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED " + objValue + " . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 1);

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals("v:age", result.get("PRED"));
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectEdgeLabelProperty() {
        String objValue = "brother";
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED \"" + objValue + "\" . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 6);

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals(result.get("PRED"), "v:label");
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectEdgeStringProperty() {
        String objValue = "loves waves";
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED \"" + objValue + "\" . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(1, resultActual.size());

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals(result.get("PRED"), "v:reason");
    }

    @Test
    public void testGotgUnboundPredicateBoundLiteralObjectEdgeIntProperty() {
        int objValue = 1;
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED " + objValue + " . }";
        GraphTraversal actual = compile(gotg, query);

        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 2);

        Map<String, Object> result = resultActual.get(0);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));
        assertEquals("v:id", result.get("PRED"));

        result = resultActual.get(1);

        assertTrue(result.containsKey("SUBJ"));
        assertTrue(result.containsKey("PRED"));

        assertEquals("v:time", result.get("PRED"));
    }

    @Test
    public void testGotgUnboundPredicateBoundUriObject() {
        String query = "SELECT DISTINCT ?SUBJ ?PRED WHERE { ?SUBJ ?PRED vid:4 . }";
        GraphTraversal actual = compile(gotg, query);
        LambdaHelper l = new LambdaHelper.NativeJavaLambdas();

        GraphTraversal expected = gg.V().match(
            __.as("SUBJ").union(
                __.outE().as("RANDOM_UUID").otherV().hasId(4)
                    .select("RANDOM_UUID").label().map(l.e()).as("PRED"),
                __.outE().as("SUBJ").otherV().hasId(4)
                    .select("SUBJ").label().map(l.eps()).as("PRED"))
            ).select("SUBJ", "PRED");

        List resultActual = actual.toList();
        List resultExpected = expected.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgOptionalWithUnboundPred() {
        GraphTraversal expected = gg.V().match(__.as("SUBJ").out("battled").as("OBJ"))
            .coalesce(__.match(__.as("OBJ").values("age").as("AGE")), (Traversal)__.constant("N/A")).as("OBJ_AGE")
            .coalesce(__.match(__.as("OBJ").values("name").as("OBJ_NAME")), (Traversal)__.constant("N/A")).as("OBJ_NAME")
            .select("SUBJ", "OBJ", "OBJ_NAME", "OBJ_AGE");

        String query =
            "SELECT ?SUBJ ?OBJ ?OBJ_NAME ?OBJ_AGE " +
            "WHERE { " +
                "?SUBJ e:battled ?OBJ . " +
                "OPTIONAL { ?OBJ v:name ?OBJ_NAME . } . " +
                "OPTIONAL { ?OBJ v:age ?OBJ_AGE . } " +
            "}";

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotg_DoubleOptional_WithUnboundPred() {
        String query =
            "SELECT ?SUBJ ?OBJ ?OBJ_NAME ?OBJ_AGE " +
            "WHERE { " +
                "?SUBJ e:brother ?OBJ . " +
                "OPTIONAL { " +
                    "?OBJ v:name ?OBJ_NAME . " +
                "} . " +
                "OPTIONAL { " +
                    "?OBJ v:age ?OBJ_AGE . " +
                    "FILTER (?OBJ_AGE >= 5000) ." +
                "} " +
            "}";

        GraphTraversal actual = compile(gotg, query);

        /*
         [
          {SUBJ=v[4], OBJ=v[5], OBJ_NAME=neptune, OBJ_AGE=N/A},
          {SUBJ=v[4], OBJ=v[8], OBJ_NAME=pluto, OBJ_AGE=N/A},
          {SUBJ=v[5], OBJ=v[4], OBJ_NAME=jupiter, OBJ_AGE=5000},
          {SUBJ=v[5], OBJ=v[8], OBJ_NAME=pluto, OBJ_AGE=N/A},
          {SUBJ=v[8], OBJ=v[4], OBJ_NAME=jupiter, OBJ_AGE=5000},
          {SUBJ=v[8], OBJ=v[5], OBJ_NAME=neptune, OBJ_AGE=N/A}
         ]
        */
        List<Map<String, Object>> resultActual = actual.toList();

        assertEquals(resultActual.size(), 6);

        for (Map<String, Object> binding : resultActual) {
            Object age = binding.get("OBJ_AGE");

            if (age instanceof Integer) {
                assertTrue((int) age >= 5000);
            } else {
                assertEquals(OPTIONAL_DEFAULT_RESULT, age);
            }
        }
    }

    @Test
    public void testGotgCommonVertex() {
        String query = "SELECT ?owner ?pet ?hero { ?owner e:pet ?pet . ?hero e:battled ?pet. }";

        GraphTraversal expected = gg.V().match(
            __.as("owner").out("pet").as("pet"),
            __.as("pet").in("battled").as("hero")
        ).select("owner", "pet", "hero");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgOrderByDecr() {
        String query =
            "SELECT ?GOD ?NAME ?AGE " +
            "WHERE { " +
                "?GOD v:label 'god' . " +
                "?GOD v:name ?NAME . " +
                "?GOD v:age ?AGE ." +
            "} " +
            "ORDER BY DESC(?AGE)";

        GraphTraversal expected = gg.V().match(
            __.as("GOD").hasLabel("god"),
            __.as("GOD").values("name").as("NAME"),
            __.as("GOD").values("age").as("AGE")
        ).order().by(__.select("AGE"), Order.desc).select("GOD", "NAME", "AGE");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgPropertiesBeforeEdges() {
        String query =
            "SELECT ?NAME ?FATHER ?CHILD ?MOTHER " +
            "WHERE { " +
            "  ?FATHER v:name ?NAME ." +
            "  ?CHILD e:father ?FATHER ." +
            "  ?CHILD e:mother ?MOTHER ." +
            "}";

        GraphTraversal expected = gg.V().match(
            __.as("CHILD").out("father").as("FATHER"),
            __.as("CHILD").out("mother").as("MOTHER"),
            __.as("FATHER").values("name").as("NAME")
        ).select("NAME", "FATHER", "CHILD", "MOTHER");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgNotEqualVarComparison() {
        String query = "SELECT ?X ?Y ?Z { ?X e:battled ?Y . ?X e:battled ?Z . FILTER(?Y != ?Z) }";

        GraphTraversal expected = gg.V().match(
            __.as("X").out("battled").as("Y"),
            __.as("X").out("battled").as("Z"),
            __.where("Y", P.neq("Z"))
        ).select("X", "Y", "Z");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgEqualVarComparison() {
        String query = "SELECT ?X ?Y ?Z { ?X e:battled ?Y . ?X e:battled ?Z . FILTER(?Y = ?Z) }";

        GraphTraversal expected = gg.V().match(
            __.as("X").out("battled").as("Y"),
            __.as("X").out("battled").as("Z"),
            __.where("Y", P.eq("Z"))
        ).select("X", "Y", "Z");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgEdgePropertiesLookup() {
        String query =
            "SELECT  ?HERO ?BADDIE ?WHEN ?WHERE " +
            "WHERE " +
            "  { ?WHO     ep:battled   ?BATTLE ;" +
            "             v:name       ?HERO ." +
            "    ?BATTLE  eps:battled  ?FOE ." +
            "    ?FOE     v:name       ?BADDIE ." +
            "    ?BATTLE  v:time       ?WHEN ;" +
            "             v:place      ?WHERE" +
            "  }";

        GraphTraversal expected = gg.V().match(
            __.as("WHO").outE("battled").as("BATTLE"),
            __.as("WHO").values("name").as("HERO"),
            __.as("BATTLE").inV().as("FOE"),
            __.as("FOE").values("name").as("BADDIE"),
            __.as("BATTLE").values("time").as("WHEN"),
            __.as("BATTLE").values("place").as("WHERE")
        ).select("HERO", "BADDIE", "WHEN", "WHERE");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgEdgePropertiesLookupConcrete() {
        String query =
            "PREFIX vid: <http://northwind.com/model/vertex-id#> " +
            "SELECT ?HERO ?BADDIE ?WHEN ?WHERE " +
            "WHERE " +
            "  { vid:6    ep:battled   ?BATTLE ;" +
            "             v:name       ?HERO ." +
            "    ?BATTLE  eps:battled  ?FOE ." +
            "    ?FOE     v:name       ?BADDIE ." +
            "    ?BATTLE  v:time       ?WHEN ;" +
            "             v:place      ?WHERE" +
            "  }";

        GraphTraversal expected = gg.V().match(
            __.as("RANDOM_UUID").hasId(6).outE("battled").as("BATTLE"),
            __.as("RANDOM_UUID").values("name").as("HERO"),
            __.as("BATTLE").inV().as("FOE"),
            __.as("FOE").values("name").as("BADDIE"),
            __.as("BATTLE").values("time").as("WHEN"),
            __.as("BATTLE").values("place").as("WHERE")
        ).select("HERO", "BADDIE", "WHEN", "WHERE");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgEdgePropertiesLookupReordered() {
        String query =
            "SELECT  ?HERO ?BADDIE ?WHEN ?WHERE " +
                "WHERE " +
                "  { ?BATTLE  eps:battled  ?FOE ." +
                "    ?WHO     ep:battled   ?BATTLE ;" +
                "             v:name       ?HERO ." +
                "    ?FOE     v:name       ?BADDIE ." +
                "    ?BATTLE  v:time       ?WHEN ;" +
                "             v:place      ?WHERE" +
                "  }";

        GraphTraversal expected = gg.V().match(
            __.as("WHO").outE("battled").as("BATTLE"),
            __.as("WHO").values("name").as("HERO"),
            __.as("BATTLE").inV().as("FOE"),
            __.as("FOE").values("name").as("BADDIE"),
            __.as("BATTLE").values("time").as("WHEN"),
            __.as("BATTLE").values("place").as("WHERE")
        ).select("HERO", "BADDIE", "WHEN", "WHERE");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
    }

    @Test
    public void testGotgAskTypeTrue() {
        // is there a father type edge going from vertex id 6 to vertex id 4 - yes
        String query = "ASK WHERE { 6 e:father 4 }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("father").hasId(4)
        );

        GraphTraversal actual = compile(gotg, query);

        boolean resultExpected = expected.hasNext();
        boolean resultActual = actual.hasNext();

        assertTrue(resultExpected);
        assertTrue(resultActual);
    }

    @Test
    public void testGotgAskTypeTrueUriPrefixed() {
        // is there a father type edge going from vertex id 6 to vertex id 4 - yes
        String query =
            "PREFIX vid: <http://northwind.com/model/vertex-id#> " +
            "ASK WHERE { vid:6 e:father vid:4 }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("father").hasId(4)
        );

        GraphTraversal actual = compile(gotg, query);

        boolean resultExpected = expected.hasNext();
        boolean resultActual = actual.hasNext();

        assertTrue(resultExpected);
        assertTrue(resultActual);
    }

    @Test
    public void testGotgAskTypeFalse() {
        // is there a father type edge going from vertex id 6 to vertex id 5 - no
        String query = "ASK WHERE { 6 e:father 5 }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("father").hasId(5)
        );

        GraphTraversal actual = compile(gotg, query);

        boolean resultExpected = expected.hasNext();
        boolean resultActual = actual.hasNext();

        assertFalse(resultExpected);
        assertFalse(resultActual);
    }

    @Test
    public void testGotgAskTypeFalseUriPrefixed() {
        // is there a father type edge going from vertex id 6 to vertex id 5 - no
        String query =
            "PREFIX vid: <http://northwind.com/model/vertex-id#> " +
            "ASK WHERE { vid:6 e:father vid:5 }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("father").hasId(5)
        );

        GraphTraversal actual = compile(gotg, query);

        boolean resultExpected = expected.hasNext();
        boolean resultActual = actual.hasNext();

        assertFalse(resultExpected);
        assertFalse(resultActual);
    }

    @Test
    public void testGotgUnoundSubjectBoundObjectPositive() {
        String query = "SELECT ?subj WHERE { ?subj e:battled 9 . }";

        GraphTraversal expected = gg.V().match(
            __.as("subj").out("battled").hasId(9)
        ).select("subj");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundSubjectBoundObjectNegative() {
        String query = "SELECT ?subj WHERE { ?subj e:battled 1 . }";

        GraphTraversal expected = gg.V().match(
            __.as("subj").out("battled").hasId(1)
        ).select("subj");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertTrue(resultExpected.isEmpty());
        assertTrue(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundSubjectBoundObjectUriNegative() {
        String query = "SELECT ?subj WHERE { ?subj e:battled <http://northwind.com/model/vertex-id#1> . }";

        GraphTraversal expected = gg.V().match(
            __.as("subj").out("battled").hasId(1)
        ).select("subj");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertTrue(resultExpected.isEmpty());
        assertTrue(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundSubjectBoundObjectUriPrefixedNegative() {
        String query =
            "PREFIX vid: <http://northwind.com/model/vertex-id#> " +
            "SELECT ?subj " +
            "WHERE { ?subj e:battled vid:1 . }";

        GraphTraversal expected = gg.V().match(
            __.as("subj").out("battled").hasId(1)
        ).select("subj");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertTrue(resultExpected.isEmpty());
        assertTrue(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundObjectBoundSubjectPositive() {
        String query = "SELECT ?monster WHERE { 6 e:battled ?monster . }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("battled").as("monster")
        ).select("monster");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundObjectBoundSubjectUriPositive() {
        String query = "SELECT ?monster WHERE { <http://northwind.com/model/vertex-id#6> e:battled ?monster . }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("battled").as("monster")
        ).select("monster");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundObjectBoundSubjectUriPrefixedPositive() {
        String query =
            "PREFIX vid: <http://northwind.com/model/vertex-id#> " +
            "SELECT ?monster WHERE { vid:6 e:battled ?monster . }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(6).out("battled").as("monster")
        ).select("monster");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    @Test
    public void testGotgUnoundObjectBoundSubjectNegative() {
        String query = "SELECT ?monster WHERE { 1 e:battled ?monster . }";

        GraphTraversal expected = gg.V().match(
            __.as(UUID.randomUUID().toString()).hasId(1).out("battled").as("monster")
        ).select("monster");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertTrue(resultExpected.isEmpty());
        assertTrue(resultActual.isEmpty());
    }

    private String[] createComparisonTestInputs(int comparable) {
        String[] inputs = new String[] {
            String.format("%s", comparable),
            String.format("xsd:float(%s)", comparable),
            String.format("xsd:int(%s)", comparable),
            String.format("xsd:integer(%s)", comparable),
            String.format("<%sabs>(%s)", ARQConstants.fnPrefix, -comparable),
            String.format("<%sstring-length>('%s')", ARQConstants.fnPrefix, StringUtils.repeat("a", comparable)),
            String.format("<%sceiling>(%s)", ARQConstants.fnPrefix, comparable - 0.9),
            String.format("<%sfloor>(%s)", ARQConstants.fnPrefix, comparable + 0.9),
            String.format("<%sround>(%s)", ARQConstants.fnPrefix, comparable + 0.4),
            String.format("<%sround>(%s)", ARQConstants.fnPrefix, comparable - 0.5),
            String.format("<%sseconds-from-duration>(xsd:duration('P5DT12H30M%s.0S'))", ARQConstants.fnPrefix, comparable),
        };

        return inputs;
    }

    private List comparisonTestHelper(String comparisonOperator, String arg1, String arg2) {
        String query =
            "SELECT ?VAR0 ?VAR1 ?VAR2 ?VAR3 " +
                "WHERE { " +
                "  ?VAR1 e:father ?VAR0 ." +
                "  ?VAR1 v:age ?VAR2 ." +
                "  FILTER (" + arg1 + " " + comparisonOperator + " " + arg2 + ")" +
                "  ?VAR1 e:mother ?VAR3 ." +
                "}";

        GraphTraversal actual = compile(gotg, query);

        return actual.toList();
    }

    @Test
    public void testGotgComparisonOperatorEquals() {
        String[] inputs = createComparisonTestInputs(30);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.eq(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg2 : inputs) {
            List resultActual = comparisonTestHelper("=", "?VAR2", arg2);

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper("=", arg1, "?VAR2");

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgComparisonOperatorNotEquals() {
        String[] inputs = createComparisonTestInputs(30);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.neq(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertTrue(resultExpected.isEmpty());

        for (String arg2 : inputs) {
            List resultActual = comparisonTestHelper("!=", "?VAR2", arg2);

            assertEquals(resultExpected, resultActual);
            assertTrue(resultActual.isEmpty());
        }

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper("!=", arg1, "?VAR2");

            assertEquals(resultExpected, resultActual);
            assertTrue(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsGreaterThan() {
        String[] inputs = createComparisonTestInputs(29);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.gt(29))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg2 : inputs) {
            List resultActual = comparisonTestHelper(">", "?VAR2", arg2);

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsGreaterThanOrEqual() {
        String[] inputs = createComparisonTestInputs(30);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.gte(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg2 : inputs) {
            List resultActual = comparisonTestHelper(">=", "?VAR2", arg2);

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsGreaterThanArgsReversed() {
        String[] inputs = createComparisonTestInputs(35);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.lt(35))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper(">", arg1, "?VAR2");

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsGreaterThanOrEqualArgsReversed() {
        String[] inputs = createComparisonTestInputs(30);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.lte(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper(">=", arg1, "?VAR2");

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsLessThan() {
        String[] inputs = createComparisonTestInputs(29);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.gt(29))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper("<", arg1, "?VAR2");

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionsLessThanArgsReversed() {
        String[] inputs = createComparisonTestInputs(35);

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.lt(35))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        List resultExpected = expected.toList();
        assertFalse(resultExpected.isEmpty());

        for (String arg1 : inputs) {
            List resultActual = comparisonTestHelper("<", "?VAR2", arg1);

            assertEquals(resultExpected, resultActual);
            assertFalse(resultActual.isEmpty());
        }
    }

    @Test
    public void testGotgXsdSingleArgFunctionLessThanOrEqual() {
        String query =
            "SELECT ?VAR0 ?VAR1 ?VAR2 ?VAR3 " +
                "WHERE { " +
                "  ?VAR1 e:father ?VAR0 ." +
                "  ?VAR1 v:age ?VAR2 ." +
                "  FILTER (?VAR2 <= xsd:float(30))" +
                "  ?VAR1 e:mother ?VAR3 ." +
                "}";

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.lte(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    @Test
    public void testGotgXsdSingleArgFunctionLessThanOrEqualArgsReversed() {
        String query =
            "SELECT ?VAR0 ?VAR1 ?VAR2 ?VAR3 " +
            "WHERE { " +
            "  ?VAR1 e:father ?VAR0 ." +
            "  ?VAR1 v:age ?VAR2 ." +
            "  FILTER (xsd:float(30) <= ?VAR2)" +
            "  ?VAR1 e:mother ?VAR3 ." +
            "}";

        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.gte(30))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");

        GraphTraversal actual = compile(gotg, query);

        List resultExpected = expected.toList();
        List resultActual = actual.toList();

        assertEquals(resultExpected, resultActual);
        assertFalse(resultExpected.isEmpty());
        assertFalse(resultActual.isEmpty());
    }

    /*
    @Before
    public void setUp() throws Exception {
        modern = TinkerFactory.createModern();
        mg = modern.traversal();
        mc = modern.traversal(computer());
        crew = TinkerFactory.createTheCrew();
        cg = modern.traversal();
        cc = modern.traversal(computer());
    }

    @Ignore
    @Test
    public void play() throws IOException {
        final String query = loadQuery("modern", 11);
        final Traversal traversal = compile(modern, query);
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println(traversal);
    }

    /* Modern */

  /*  @Test
    public void testModern1() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30))).
                select("a", "b", "c");
        assertEquals(expected, compile(modern, loadQuery("modern", 1)));
    }

    @Test
    public void testModern2() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30)));
        assertEquals(expected, compile(modern, loadQuery("modern", 2)));
    }

    @Test
    public void testModern3() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).select("name", "age");
        assertEquals(expected, compile(modern, loadQuery("modern", 3)));
    }

    @Test
    public void testModern4() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age"),
                as("person").out("created").as("project"),
                as("project").values("name").is("lop")).select("name", "age");
        assertEquals(expected, compile(modern, loadQuery("modern", 4)));
    }

    @Test
    public void testModern5() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").is(29)).select("name");
        assertEquals(expected, compile(modern, loadQuery("modern", 5)));
    }

    @Test
    public void testModern6() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(and(as("age").is(gt(30)), as("age").is(lt(40)))).
                select("name", "age");
        assertEquals(expected, compile(modern, loadQuery("modern", 6)));
    }

    @Test
    public void testModern7() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(or(as("age").is(lt(30)), as("age").is(gt(40)))).
                select("name", "age");
        assertEquals(expected, compile(modern, loadQuery("modern", 7)));
    }

    @Test
    public void testModern8() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(
                or(and(as("age").is(gt(30)), as("age").is(lt(40))), as("name").is("marko"))).
                select("name", "age");
        assertEquals(expected, compile(modern, loadQuery("modern", 8)));
    }

    @Test
    public void testModern9() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(as("a").values("age")).
                select("name");
        assertEquals(expected, compile(modern, loadQuery("modern", 9)));
    }

    @Test
    public void testModern10() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(__.not(as("a").values("age"))).
                select("name");
        assertEquals(expected, compile(modern, loadQuery("modern", 10)));
    }

    @Test
    public void testModern11() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("name")).dedup("name").select("name");
        assertEquals(expected, compile(modern, loadQuery("modern", 11)));
    }

    @Test
    public void testModern12() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("b").values("name").as("name")).dedup();
        assertEquals(expected, compile(modern, loadQuery("modern", 12)));
    }

    @Test
    public void testModern13() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("c")).dedup("a", "b", "c").select("a", "b", "c");
        assertEquals(expected, compile(modern, loadQuery("modern", 13)));
    }

    /* The Crew */

 /*   @Test
    public void testCrew1() throws Exception {
        final GraphTraversal expected = cg.V().match(
                as("a").values("name").is("daniel"),
                as("a").properties("location").as("b"),
                as("b").value().as("c"),
                as("b").values("startTime").as("d")).
                select("c", "d");
        assertEquals(expected, compile(crew, loadQuery("crew", 1)));
    }

    /* Computer Mode */

  /*  @Test
    public void testModernInComputerMode() throws Exception {
        final GraphTraversal expected = mc.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30))).
                select("a", "b", "c");
        assertEquals(expected, compile(mc, loadQuery("modern", 1)));
    }

    @Test
    public void testCrewInComputerMode() throws Exception {
        final GraphTraversal expected = cc.V().match(
                as("a").values("name").is("daniel"),
                as("a").properties("location").as("b"),
                as("b").value().as("c"),
                as("b").values("startTime").as("d")).
                select("c", "d");
        assertEquals(expected, compile(crew, loadQuery("crew", 1)));
    }*/
}