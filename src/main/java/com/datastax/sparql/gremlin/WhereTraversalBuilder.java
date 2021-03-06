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
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.E_StrLength;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.Function;
import org.apache.jena.sparql.function.FunctionBase;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class WhereTraversalBuilder {

//    public static GraphTraversal<?, ?> transformExprFunction2(final ExprFunction2 expression, Function<Object, P<String>> func2) {
//        String arg1VarName = expression.getArg1().getVarName();
//        Expr arg2 = expression.getArg2();
//
//        if (arg2.isConstant()) {
//            Object value = arg2.getConstant().getNode().getLiteralValue();
//            return __.as(arg1VarName).is(func2.apply(value));
//        } else if (arg2.isVariable()) {
//            String arg2VarName = arg2.getVarName();
//            return __.as(arg1VarName).where(arg1VarName, func2.apply(arg2VarName));
//        } else {
//            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
//        }
//    }

    // todo: experiment with supporting 'math' step via FunctionRegistry
    private static NodeValue execFunc(ExprFunction fn) {
        FunctionRegistry fr = FunctionRegistry.get();
        String fnUri = fn.getFunctionIRI();

        if (!fr.isRegistered(fnUri)) {
            throw new IllegalStateException(String.format("Unsupported function: %s", fn.getFunctionIRI()));
        }

        FunctionFactory fnFactory = fr.get(fnUri);
        Function eFn = fnFactory.create(fnUri);

        List<Expr> fnArgs = fn.getArgs();
        List<NodeValue> args = new ArrayList<>(fnArgs.size());

        for (Expr fnArg : fnArgs) {
            if (fnArg.isConstant()) {
                args.add(fnArg.getConstant());
            } else if (fnArg.isFunction()) {
                args.add(execFunc((ExprFunction) fnArg));
            } else {
                throw new IllegalStateException(String.format("Unsupported function argument: %s", fn.getArgs()));
            }
        }

        if (eFn instanceof FunctionBase) {
            return ((FunctionBase) eFn).exec(args);
        } else {
            throw new IllegalStateException(String.format("Unsupported function: %s", fn.getFunctionIRI()));
        }
    }

    /*
     * Equals support
     */

    public static GraphTraversal<?, ?> transform(final E_Equals expression) {
        // also handle ( xsd:float(29) < ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformEquals(arg1 ,arg2);
        } else if (arg2.isVariable()) {
            return transformEquals(arg2, arg1);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformEquals(final Expr arg1, final Expr arg2) {
//        return transformExprFunction2(expression, (value) -> new P(Compare.eq, value));
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Node node = arg2.getConstant().getNode();
            if (node.isLiteral()) {
                Object value = node.getLiteralValue();
                return __.as(arg1VarName).is(P.eq(value));
            } else if (node.isURI()) {
                String uri = node.getURI();

                if (Prefixes.isValidVertexIdUri(uri)) {
                    String uriValue = Prefixes.getURIValue(uri);
                    return __.as(arg1VarName).hasId(uriValue);
                }
            }
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.eq(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.eq(value));
        }

        throw new IllegalStateException(String.format("Unhandled Equals expression: %s %s", arg1, arg2));
    }

    /*
     * NotEquals support
     */

    public static GraphTraversal<?, ?> transform(final E_NotEquals expression) {
        // also handle ( xsd:float(29) < ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformNotEquals(arg1 ,arg2);
        } else if (arg2.isVariable()) {
            return transformNotEquals(arg2, arg1);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformNotEquals(final Expr arg1, final Expr arg2) {
//        return transformExprFunction2(expression, (value) -> new P(Compare.eq, value));
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Node node = arg2.getConstant().getNode();
            if (node.isLiteral()) {
                Object value = node.getLiteralValue();
                return __.as(arg1VarName).is(P.neq(value));
            } else if (node.isURI()) {
                String uri = node.getURI();

                if (Prefixes.isValidVertexIdUri(uri)) {
                    String uriValue = Prefixes.getURIValue(uri);
                    return __.as(arg1VarName).not(__.hasId(uriValue));
                }
            }
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.neq(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.neq(value));
        }

        throw new IllegalStateException(String.format("Unhandled NotEquals expression: %s %s", arg1, arg2));
    }

    /*
     * LessThan support
     */

    public static GraphTraversal<?, ?> transform(final E_LessThan expression) {
        // also handle ( xsd:float(29) < ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformArg1Var(expression);
        } else if (arg2.isVariable()) {
            return transformArg2Var(expression);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transformArg1Var(final E_LessThan expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lt(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.lt(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transformArg2Var(final E_LessThan expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg2VarName = arg2.getVarName();

        if (arg1.isConstant()) {
            Object value = arg1.getConstant().getNode().getLiteralValue();
            return __.as(arg2VarName).is(P.gt(value));
        } else if (arg1.isVariable()) {
            String arg1VarName = arg1.getVarName();
            return __.as(arg2VarName).where(arg2VarName, P.gt(arg1VarName));
        } else if (arg1.isFunction()) {
            ExprFunction fn = arg1.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg2VarName).is(P.gt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    /*
     * LessThanOrEqual support
     */

    public static GraphTraversal<?, ?> transform(final E_LessThanOrEqual expression) {
        // also handle ( xsd:float(29) <= ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformArg1Var(expression);
        } else if (arg2.isVariable()) {
            return transformArg2Var(expression);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transformArg1Var(final E_LessThanOrEqual expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lte(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.lte(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transformArg2Var(final E_LessThanOrEqual expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg2VarName = arg2.getVarName();

        if (arg1.isConstant()) {
            Object value = arg1.getConstant().getNode().getLiteralValue();
            return __.as(arg2VarName).is(P.gte(value));
        } else if (arg1.isVariable()) {
            String arg1VarName = arg1.getVarName();
            return __.as(arg2VarName).where(arg2VarName, P.gte(arg1VarName));
        } else if (arg1.isFunction()) {
            ExprFunction fn = arg1.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg2VarName).is(P.gte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    /*
     * GreaterThan support
     */

    public static GraphTraversal<?, ?> transform(final E_GreaterThan expression) {
        // also handle ( xsd:float(29) > ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformArg1Var(expression);
        } else if (arg2.isVariable()) {
            return transformArg2Var(expression);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformArg2Var(E_GreaterThan expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg2VarName = arg2.getVarName();

        if (arg1.isConstant()) {
            Object value = arg1.getConstant().getNode().getLiteralValue();
            return __.as(arg2VarName).is(P.lt(value));
        } else if (arg1.isVariable()) {
            String arg1VarName = arg1.getVarName();
            return __.as(arg2VarName).where(arg2VarName, P.lt(arg1VarName));
        } else if (arg1.isFunction()) {
            ExprFunction fn = arg1.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg2VarName).is(P.lt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformArg1Var(E_GreaterThan expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gt(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.gt(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    /*
     * GreaterThanOrEqual support
     */

    public static GraphTraversal<?, ?> transform(final E_GreaterThanOrEqual expression) {
        // also handle ( xsd:float(29) > ?VAR2 ) -like expressions
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();

        if (arg1.isVariable()) {
            return transformArg1Var(expression);
        } else if (arg2.isVariable()) {
            return transformArg2Var(expression);
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformArg1Var(final E_GreaterThanOrEqual expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg1VarName = arg1.getVarName();

        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gte(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.gte(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    private static GraphTraversal<?, ?> transformArg2Var(E_GreaterThanOrEqual expression) {
        Expr arg1 = expression.getArg1();
        Expr arg2 = expression.getArg2();
        String arg2VarName = arg2.getVarName();

        if (arg1.isConstant()) {
            Object value = arg1.getConstant().getNode().getLiteralValue();
            return __.as(arg2VarName).is(P.lte(value));
        } else if (arg1.isVariable()) {
            String arg1VarName = arg1.getVarName();
            return __.as(arg2VarName).where(arg2VarName, P.lte(arg1VarName));
        } else if (arg1.isFunction()) {
            ExprFunction fn = arg1.getFunction();
            NodeValue fnResult = execFunc(fn);
            Object value = fnResult.asNode().getLiteralValue();
            return __.as(arg2VarName).is(P.lte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transform(final E_LogicalAnd expression, Map<Object, UUID> vertexIdToUuid) {
        return __.and(
            transform(expression.getArg1(), vertexIdToUuid),
            transform(expression.getArg2(), vertexIdToUuid));
    }

    public static GraphTraversal<?, ?> transform(final E_LogicalOr expression, Map<Object, UUID> vertexIdToUuid) {
        return __.or(
            transform(expression.getArg1(), vertexIdToUuid),
            transform(expression.getArg2(), vertexIdToUuid));
    }

    public static GraphTraversal<?, ?> transform(final E_Exists expression, Map<Object, UUID> vertexIdToUuid) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0), false, vertexIdToUuid);
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return traversal;
    }


    public static GraphTraversal<?, ?> transform(final E_NotExists expression, Map<Object, UUID> vertexIdToUuid) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled NOT EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0), false, vertexIdToUuid);
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return __.not(traversal);
    }

    public static int getStrLength(final E_StrLength expression) {

        return expression.getArg().toString().length();

    }


    //what does <?, ?> signify? possibly <S,E>
    public static GraphTraversal<?, ?> transform(final Expr expression, Map<Object, UUID> vertexIdToUuid) {
        if (expression instanceof E_Equals) return transform((E_Equals) expression);
        if (expression instanceof E_NotEquals) return transform((E_NotEquals) expression);
        if (expression instanceof E_LessThan) return transform((E_LessThan) expression);
        if (expression instanceof E_LessThanOrEqual) return transform((E_LessThanOrEqual) expression);
        if (expression instanceof E_GreaterThan) return transform((E_GreaterThan) expression);
        if (expression instanceof E_GreaterThanOrEqual) return transform((E_GreaterThanOrEqual) expression);
        if (expression instanceof E_LogicalAnd) return transform((E_LogicalAnd) expression, vertexIdToUuid);
        if (expression instanceof E_LogicalOr) return transform((E_LogicalOr) expression, vertexIdToUuid);
        if (expression instanceof E_Exists) return transform((E_Exists) expression, vertexIdToUuid);
        if (expression instanceof E_NotExists) return transform((E_NotExists) expression, vertexIdToUuid);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }
}
