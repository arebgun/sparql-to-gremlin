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
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.query.Syntax;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparqlToGremlinCompiler {

	private GraphTraversal<Vertex, ?> traversal;

	List<Traversal> traversalList = new ArrayList<>();
	List<Traversal> optionalTraversals = new ArrayList<>();
	List<String> optionalVariable = new ArrayList<>();
    Map<Object, UUID> vertexIdToUuid = new HashMap<>();

    boolean optionalFlag = false;

	GraphTraversalSource temp;
	Graph graph;

	private SparqlToGremlinCompiler(final GraphTraversal<Vertex, ?> traversal) {
		this.traversal = traversal;
	}

	private SparqlToGremlinCompiler(final GraphTraversalSource g) {
		this(g.V());
		temp = g;

	}

	private SparqlToGremlinCompiler(final Graph g) {
		this.traversal = (GraphTraversal<Vertex, ?>) g.traversal();
		graph = g;
	}

	private GraphTraversal<Vertex, ?> compile(final Query query) {
		final Op op = Algebra.compile(query);
		OpWalker.walk(op, new GremlinOpVisitor());

		int traversalIndex = 0;
		final int numberOfTraversal = traversalList.size();
		final int numberOfOptionalTraversal = optionalTraversals.size();
		final Traversal[] arrayOfAllTraversals = (numberOfOptionalTraversal > 0) ?
			new Traversal[numberOfTraversal - numberOfOptionalTraversal + 1] :
			new Traversal[numberOfTraversal - numberOfOptionalTraversal];

		final Traversal[] arrayOfOptionalTraversals = new Traversal[numberOfOptionalTraversal];

		for (Traversal tempTrav : traversalList) {
			arrayOfAllTraversals[traversalIndex++] = tempTrav;
		}

		traversalIndex = 0;
		for (Traversal tempTrav : optionalTraversals)
			arrayOfOptionalTraversals[traversalIndex++] = tempTrav;

		// creates a map of ordering keys and their ordering direction
		final Map<String, Order> orderingIndex = createOrderIndexFromQuery(query);

		if (traversalList.size() > 0)
			traversal = traversal.match(arrayOfAllTraversals);

		if (optionalTraversals.size() > 0) {
			traversal = traversal.coalesce(__.match(arrayOfOptionalTraversals), (Traversal) __.constant("N/A"));
			for (int i = 0; i < optionalVariable.size(); i++) {
				traversal = traversal.as(optionalVariable.get(i).substring(1));
			}
		}

		final List<String> vars = query.getResultVars();
		if (!query.isQueryResultStar() && !query.hasGroupBy() && !query.isAskType()) {
			final String[] all = new String[vars.size()];
			vars.toArray(all);
			if (query.isDistinct()) {
				traversal = traversal.dedup(all);
			}

			// apply ordering from ORDER BY
			orderingIndex.forEach((k, v) -> traversal = traversal.order().by(__.select(k), v));

			// the result sizes have special handling to get the right signatures of select() called.
			switch (all.length) {
				case 0:
					throw new IllegalStateException();
				case 1:
					traversal = traversal.select(all[0]);
					break;
				case 2:
					traversal = traversal.select(all[0], all[1]);
					break;
				default:
					final String[] others = Arrays.copyOfRange(all, 2, vars.size());
					traversal = traversal.select(all[0], all[1], others);
					break;
			}
		}

		if (query.hasGroupBy()) {
			final VarExprList lstExpr = query.getGroupBy();
			String grpVar = "";
			for (Var expr : lstExpr.getVars()) {
				grpVar = expr.getName();
			}

			if (!grpVar.isEmpty())
				traversal = traversal.select(grpVar);
			if (query.hasAggregators()) {
				final List<ExprAggregator> exprAgg = query.getAggregators();
				for (ExprAggregator expr : exprAgg) {
					if (expr.getAggregator().getName().contains("COUNT")) {
						if (!query.toString().contains("GROUP")) {
							if (expr.getAggregator().toString().contains("DISTINCT"))
								traversal = traversal.dedup(expr.getAggregator().getExprList().get(0).toString().substring(1));
							else
								traversal = traversal.select(expr.getAggregator().getExprList().get(0).toString().substring(1));

							traversal = traversal.count();
						} else {
							traversal = traversal.groupCount();
						}
					}
					if (expr.getAggregator().getName().contains("MAX")) {
						traversal = traversal.max();
					}
				}
			} else {
				traversal = traversal.group();
			}
		}

		if (query.hasOrderBy() && query.hasGroupBy())
			orderingIndex.forEach((k, v) -> traversal = traversal.order().by(__.select(k), v));

		if (query.hasLimit()) {
			long limit = query.getLimit(), offset = 0;

			if (query.hasOffset())
				offset = query.getOffset();

			if (query.hasGroupBy() && query.hasOrderBy())
				traversal = traversal.range(Scope.local, offset, offset + limit);
			else
				traversal = traversal.range(offset, offset + limit);
		}

		return traversal;
	}

	/**
	 * Extracts any {@code SortCondition} instances from the SPARQL query and holds them in an index of their keys
	 * where the value is that keys sorting direction.
	 */
	private static Map<String, Order> createOrderIndexFromQuery(final Query query) {
		final Map<String, Order> orderingIndex = new HashMap<>();
		if (query.hasOrderBy()) {
			final List<SortCondition> sortingConditions = query.getOrderBy();

			for (SortCondition sortCondition : sortingConditions) {
				final Expr expr = sortCondition.getExpression();

				// by default, the sort will be ascending. getDirection() returns -2 if the DESC/ASC isn't
				// supplied - weird
				orderingIndex.put(expr.getVarName(), sortCondition.getDirection() == -1 ? Order.decr : Order.incr);
			}
		}

		return orderingIndex;
	}

	/**
	 * An {@code OpVisitor} implementation that reads SPARQL algebra operations into Gremlin traversals.
	 */
	private class GremlinOpVisitor extends OpVisitorBase {

		/**
		 * Visiting triple patterns in SPARQL algebra.
		 */
		@Override
		public void visit(final OpBGP opBGP) {
			final Set<Node> visitedNodes = new HashSet<>();

			List<Triple> triples = opBGP.getPattern().getList();
			triples = reorderTriplesWrtProperties(triples);

			if(optionalFlag)
			{
				triples.forEach(triple -> optionalTraversals.add(TraversalBuilder.transform(triple, tripleRequiresEdgeInversion(triple, visitedNodes), vertexIdToUuid)));
				triples.forEach(triple -> optionalVariable.add(triple.getObject().toString()));

			}
			else {
				triples.forEach(triple -> traversalList.add(TraversalBuilder.transform(triple, tripleRequiresEdgeInversion(triple, visitedNodes), vertexIdToUuid)));
			}
		}

		/**
		 * Visiting filters in SPARQL algebra.
		 */
		@Override
		public void visit(final OpFilter opFilter) {
			Traversal traversal;
			for (Expr expr : opFilter.getExprs().getList()) {
				if (expr != null) {
					traversal = __.where(WhereTraversalBuilder.transform(expr, vertexIdToUuid));
					traversalList.add(traversal);
				}
			}
		}


		/**
		 * Visiting LeftJoin(Optional) in SPARQL algebra.
		 */
		@Override
		public void visit(final OpLeftJoin opLeftJoin) {
			optionalFlag = true;
			optionalVisit(opLeftJoin.getRight());
			if (opLeftJoin.getExprs() != null) {
				for (Expr expr : opLeftJoin.getExprs().getList()) {
					if (expr != null) {
						if (optionalFlag)
							optionalTraversals.add(__.where(WhereTraversalBuilder.transform(expr, vertexIdToUuid)));
					}
				}
			}
		}

		/**
		 * Walking OP for Optional in SPARQL algebra.
		 */
		private void optionalVisit(final Op op) {

			OpWalker.walk(op, this);
		}

		/**
		 * Visiting unions in SPARQL algebra.
		 */
		@Override
		public void visit(final OpUnion opUnion) {
			final Traversal unionTemp[] = new Traversal[2];
			final Traversal unionTemp1[] = new Traversal[traversalList.size() / 2];
			final Traversal unionTemp2[] = new Traversal[traversalList.size() / 2];

			int count = 0;

			for (int i = 0; i < traversalList.size(); i++) {
				if (i < traversalList.size() / 2)
					unionTemp1[i] = traversalList.get(i);
				else
					unionTemp2[count++] = traversalList.get(i);
			}

			unionTemp[1] = __.match(unionTemp2);
			unionTemp[0] = __.match(unionTemp1);

			traversalList.clear();
			traversal = (GraphTraversal<Vertex, ?>) traversal.union(unionTemp);
		}

	}

	public static GraphTraversal<Vertex, ?> convertToGremlinTraversal(final Graph graph, final String query) {
		return compile(graph, query);
	}

	public static GraphTraversal<Vertex, ?> compile(final Graph graph, final String query) {
		if (query.contains("?x") && query.contains("?y") && query.contains("?z")) {
			return graph.traversal().V().outE().inV().path().by(__.valueMap());
		}

		String pquery = Prefixes.prepend(query);
		ParameterizedSparqlString pss = new ParameterizedSparqlString(pquery, PrefixMapping.Standard);
		Query q = QueryFactory.create(pss.toString(), Syntax.syntaxSPARQL);
		return new SparqlToGremlinCompiler(graph.traversal()).compile(q);
	}

    private List<Triple> reorderTriplesWrtProperties(List<Triple> triples) {
        List<Triple> edges = new ArrayList<>();
        List<Triple> eps = new ArrayList<>();
        List<Triple> propsAndValues = new ArrayList<>();

        for (Triple t : triples) {
            Node predicate = t.getPredicate();

            if (predicate.isURI()) {
                final String uri = predicate.getURI();
                final String uriValue = Prefixes.getURIValue(uri);
                final String prefix = Prefixes.getPrefix(uri);

                switch (prefix) {
                    case "edge":
                    case "edge-proposition":
                        edges.add(t);
                        break;
                    case "edge-proposition-subject":
                        eps.add(t);
                        break;
                    case "property":
                    case "value":
                        propsAndValues.add(t);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unexpected predicate: %s", predicate));
                }
            } else {
                edges.add(t);
            }
        }
        return Stream.concat(Stream.concat(edges.stream(), eps.stream()), propsAndValues.stream()).collect(Collectors.toList());
    }

    private boolean tripleRequiresEdgeInversion(Triple triple, Set<Node> visitedNodes) {
        Node predicate = triple.getPredicate();

        if (predicate.isURI()) {
            String uri = predicate.getURI();
            String prefix = Prefixes.getPrefix(uri);

            // ?X pred ?Y
            // ?Z pred ?Y
            if (prefix.equalsIgnoreCase("edge")) {
                Node subject = triple.getSubject();
                Node object = triple.getObject();

                if (visitedNodes.isEmpty()) {
                    visitedNodes.add(subject);
                    visitedNodes.add(object);
                    return false;
                } else if (visitedNodes.contains(subject)) {
                    visitedNodes.add(object);
                    return false;
                } else {
                    visitedNodes.add(subject);
                    return true;
                }
            }
        }

        return false;
    }
}
