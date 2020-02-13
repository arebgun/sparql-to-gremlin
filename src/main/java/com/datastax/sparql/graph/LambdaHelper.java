package com.datastax.sparql.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Predicate;

public interface LambdaHelper {
    enum Type {
        LOCAL,
        REMOTE
    }

    Function<Traverser<String>, Object> v();
    Function<Traverser<String>, Object> e();
    Function<Traverser<String>, Object> p();
    Function<Traverser<String>, Object> ep();
    Function<Traverser<String>, Object> eps();
    Function<Traverser<String>, Object> epAndEps();
    Predicate<Object> isVertex();
    Predicate<Object> isEdge();
    Predicate<Object> isProperty();
    Function<?, Object> isElement();
    Function isVertexFn();
    Function getType();

    class NativeJavaLambdas implements LambdaHelper {
        @Override
        public Function<Traverser<String>, Object> v() {
            return it -> "v:" + it;
        }

        @Override
        public Function<Traverser<String>, Object> e() {
            return it -> "e:" + it;
        }

        @Override
        public Function<Traverser<String>, Object> p() {
            return it -> "p:" + it;
        }

        @Override
        public Function<Traverser<String>, Object> ep() {
            return it -> "ep:" + it;
        }

        @Override
        public Function<Traverser<String>, Object> eps() {
            return it -> "eps:" + it;
        }

        @Override
        public Function<Traverser<String>, Object> epAndEps() {
            return it -> Arrays.asList("ep:" + it, "eps:" + it);
        }

        @Override
        public Predicate<Object> isVertex() {
            return it -> it instanceof Vertex;
        }

        @Override
        public Predicate<Object> isEdge() {
            return it -> it instanceof Edge;
        }

        @Override
        public Predicate<Object> isProperty() {
            return it -> it instanceof Property;
        }

        @Override
        public Function<?, Object> isElement() {
            return it -> it instanceof Element;
        }

        @Override
        public Function isVertexFn() {
            return it -> it instanceof Vertex;
        }

        @Override
        public Function getType() {
            return it -> {
                if (it instanceof Vertex) return "vertex";
                else if (it instanceof Edge) return "edge";
                else if (it instanceof Property) return "property";
                else return "unknown";
            };
        }
    }

    class RemoteGroovyLambdas implements LambdaHelper {
        @Override
        public Function<Traverser<String>, Object> v() {
            return Lambda.function("'v:' + it");
        }

        @Override
        public Function<Traverser<String>, Object> e() {
            return Lambda.function("'e:' + it");
        }

        @Override
        public Function<Traverser<String>, Object> p() {
            return Lambda.function("'p:' + it");
        }

        @Override
        public Function<Traverser<String>, Object> ep() {
            return Lambda.function("'ep:' + it");
        }

        @Override
        public Function<Traverser<String>, Object> eps() {
            return Lambda.function("'eps:' + it");
        }

        @Override
        public Function<Traverser<String>, Object> epAndEps() {
            return Lambda.function("['ep:' + it, 'eps:' + it]");
        }

        @Override
        public Predicate<Object> isVertex() {
            return Lambda.predicate("it instanceof Vertex");
        }

        @Override
        public Predicate<Object> isEdge() {
            return Lambda.predicate("it instanceof Edge");
        }

        @Override
        public Predicate<Object> isProperty() {
            return Lambda.predicate("it instanceof Property");
        }

        @Override
        public Function<?, Object> isElement() {
            return Lambda.function("it instanceof Element");
        }

        @Override
        public Function isVertexFn() {
            return Lambda.function("it instanceof Vertex");
        }

        @Override
        public Function getType() {
            return Lambda.function(
                "if (it instanceof Vertex) return 'vertex';" +
                " else if (it instanceof Edge) return 'edge';" +
                " else if (it instanceof Property) return 'property';" +
                " else return 'unknown';");
        }
    }
}
