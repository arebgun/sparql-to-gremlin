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

import java.util.List;
import java.util.function.Consumer;

import org.apache.jena.datatypes.xsd.XSDDatatype;
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
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.function.Function;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

class WhereTraversalBuilder {
    
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
    
    public static GraphTraversal<?, ?> transform(final E_Equals expression) {
//        return transformExprFunction2(expression, (value) -> new P(Compare.eq, value));
        String arg1VarName = expression.getArg1().getVarName();
        Expr arg2 = expression.getArg2();

        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.eq(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.eq(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.eq(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transform(final E_NotEquals expression) {
        String arg1VarName = expression.getArg1().getVarName();
        Expr arg2 = expression.getArg2();
        
        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.neq(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.neq(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.neq(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transform(final E_LessThan expression) {
        String arg1VarName = expression.getArg1().getVarName();
        Expr arg2 = expression.getArg2();
    
        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lt(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.lt(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transform(final E_LessThanOrEqual expression) {
        String arg1VarName = expression.getArg1().getVarName();
        Expr arg2 = expression.getArg2();
    
        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lte(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.lte(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.lte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

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
            String arg1VarName = arg2.getVarName();
            return __.as(arg2VarName).where(arg2VarName, P.lt(arg1VarName));
        } else if (arg1.isFunction()) {
            ExprFunction fn = arg1.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
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
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gt(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }
    
    public static GraphTraversal<?, ?> transform(final E_GreaterThanOrEqual expression) {
        String arg1VarName = expression.getArg1().getVarName();
        Expr arg2 = expression.getArg2();
    
        if (arg2.isConstant()) {
            Object value = arg2.getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gte(value));
        } else if (arg2.isVariable()) {
            String arg2VarName = arg2.getVarName();
            return __.as(arg1VarName).where(arg1VarName, P.gte(arg2VarName));
        } else if (arg2.isFunction()) {
            ExprFunction fn = arg2.getFunction();
            Object value = fn.getArg(1).getConstant().getNode().getLiteralValue();
            return __.as(arg1VarName).is(P.gte(value));
        } else {
            throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
        }
    }

    public static GraphTraversal<?, ?> transform(final E_LogicalAnd expression) {
        return __.and(
                transform(expression.getArg1()),
                transform(expression.getArg2()));
    }

    public static GraphTraversal<?, ?> transform(final E_LogicalOr expression) {
        return __.or(
                transform(expression.getArg1()),
                transform(expression.getArg2()));
    }

    public static GraphTraversal<?, ?> transform(final E_Exists expression) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0), false);
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return traversal;
    }
    

    public static GraphTraversal<?, ?> transform(final E_NotExists expression) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled NOT EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0), false);
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return __.not(traversal);
    }
    
    public static int getStrLength(final E_StrLength expression){
    	
    	return expression.getArg().toString().length();
    	
    }
    
    
    //what does <?, ?> signify? possibly <S,E>
    public static GraphTraversal<?, ?> transform(final Expr expression) {
        if (expression instanceof E_Equals) return transform((E_Equals) expression);
        if (expression instanceof E_NotEquals) return transform((E_NotEquals) expression);
        if (expression instanceof E_LessThan) return transform((E_LessThan) expression);
        if (expression instanceof E_LessThanOrEqual) return transform((E_LessThanOrEqual) expression);
        if (expression instanceof E_GreaterThan) return transform((E_GreaterThan) expression);
        if (expression instanceof E_GreaterThanOrEqual) return transform((E_GreaterThanOrEqual) expression);
        if (expression instanceof E_LogicalAnd) return transform((E_LogicalAnd) expression);
        if (expression instanceof E_LogicalOr) return transform((E_LogicalOr) expression);
        if (expression instanceof E_Exists) return transform((E_Exists) expression);
        if (expression instanceof E_NotExists) return transform((E_NotExists) expression);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }
}
