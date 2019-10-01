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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.datastax.sparql.graph.GraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;

import static com.datastax.sparql.gremlin.SparqlToGremlinCompiler.convertToGremlinTraversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

public class SparqlToGremlinCompilerTest {

    private Graph modern, crew, gotg;
    private GraphTraversalSource mg, cg, gg;
    private GraphTraversalSource mc, cc;
    
    @Before
    public void setUp() throws Exception {
        gotg = GraphFactory.createGraphOfTheGods();
        gg = gotg.traversal();
    }
    
    @Test
    public void testGotgCommonVertex() {
        String query = "SELECT ?owner ?pet ?hero { ?owner e:pet ?pet . ?hero e:battled ?pet. }";
        
        GraphTraversal expected = gg.V().match(
            __.as("owner").out("pet").as("pet"),
            __.as("pet").in("battled").as("hero")
        ).select("owner", "pet", "hero");
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
    
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
        List resultExpected = expected.toList();
        List resultActual = actual.toList();
    
        assertEquals(resultExpected, resultActual);
    }
    
    @Test
    public void testGotgVarComparison() {
        String query = "SELECT ?X ?Y ?Z { ?X e:battled ?Y . ?X e:battled ?Z . FILTER(?Y != ?Z) }";
        
        GraphTraversal expected = gg.V().match(
            __.as("X").out("battled").as("Y"),
            __.as("X").out("battled").as("Z"),
            __.where("Y", P.neq("Z"))
        ).select("X", "Y", "Z");
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
    
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
        
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
        
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
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
        
        List resultExpected = expected.toList();
        List resultActual = actual.toList();
        
        assertEquals(resultExpected, resultActual);
        assertTrue(resultExpected.isEmpty());
        assertTrue(resultActual.isEmpty());
    }
    
    @Test
    public void testGotgXsdSingleArgFunctions() {
        String query =
            "SELECT ?VAR0 ?VAR1 ?VAR2 ?VAR3 " +
            "WHERE { " +
            "  ?VAR1 e:father ?VAR0 ." +
            "  ?VAR1 v:age ?VAR2 ." +
            "  FILTER (?VAR2 > xsd:float(29))" +
            "  ?VAR1 e:mother ?VAR3 ." +
            "}";
        
        GraphTraversal expected = gg.V().match(
            __.as("VAR1").out("father").as("VAR0"),
            __.as("VAR1").out("mother").as("VAR3"),
            __.as("VAR1").values("age").as("VAR2"),
            __.as("VAR2").is(P.gt(29))
        ).select("VAR0", "VAR1", "VAR2", "VAR3");
        
        GraphTraversal actual = convertToGremlinTraversal(gotg, query);
    
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
        final Traversal traversal = convertToGremlinTraversal(modern, query);
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
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 1)));
    }

    @Test
    public void testModern2() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30)));
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 2)));
    }

    @Test
    public void testModern3() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 3)));
    }

    @Test
    public void testModern4() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age"),
                as("person").out("created").as("project"),
                as("project").values("name").is("lop")).select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 4)));
    }

    @Test
    public void testModern5() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").is(29)).select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 5)));
    }

    @Test
    public void testModern6() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(and(as("age").is(gt(30)), as("age").is(lt(40)))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 6)));
    }

    @Test
    public void testModern7() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(or(as("age").is(lt(30)), as("age").is(gt(40)))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 7)));
    }

    @Test
    public void testModern8() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(
                or(and(as("age").is(gt(30)), as("age").is(lt(40))), as("name").is("marko"))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 8)));
    }

    @Test
    public void testModern9() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(as("a").values("age")).
                select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 9)));
    }

    @Test
    public void testModern10() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(__.not(as("a").values("age"))).
                select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 10)));
    }

    @Test
    public void testModern11() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("name")).dedup("name").select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 11)));
    }

    @Test
    public void testModern12() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("b").values("name").as("name")).dedup();
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 12)));
    }

    @Test
    public void testModern13() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("c")).dedup("a", "b", "c").select("a", "b", "c");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 13)));
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
        assertEquals(expected, convertToGremlinTraversal(crew, loadQuery("crew", 1)));
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
        assertEquals(expected, convertToGremlinTraversal(mc, loadQuery("modern", 1)));
    }

    @Test
    public void testCrewInComputerMode() throws Exception {
        final GraphTraversal expected = cc.V().match(
                as("a").values("name").is("daniel"),
                as("a").properties("location").as("b"),
                as("b").value().as("c"),
                as("b").values("startTime").as("d")).
                select("c", "d");
        assertEquals(expected, convertToGremlinTraversal(crew, loadQuery("crew", 1)));
    }*/
}