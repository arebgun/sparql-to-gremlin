package com.datastax.sparql.gremlin;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.Op2;

public class GremlinatorOpWalker extends OpWalker {
    public static void walk(Op op, OpVisitor visitor)
    {
        walk(new GremlinatorWalkerVisitor(visitor, null, null), op) ;
    }

    public static void walk(Op op, OpVisitor visitor, OpVisitor beforeVisitor, OpVisitor afterVisitor)
    {
        walk(new GremlinatorWalkerVisitor(visitor, beforeVisitor, afterVisitor), op) ;
    }

    public static class GremlinatorWalkerVisitor extends WalkerVisitor {
        public GremlinatorWalkerVisitor(OpVisitor visitor, OpVisitor beforeVisitor, OpVisitor afterVisitor) {
            super(visitor, beforeVisitor, afterVisitor);
        }

        public GremlinatorWalkerVisitor(OpVisitor visitor) {
            super(visitor);
        }

        @Override
        protected void visit2(Op2 op) {
            before(op) ;
            if ( op.getLeft() != null )
                op.getLeft().visit(this) ;
//            if ( op.getRight() != null )
//                op.getRight().visit(this) ;
            if ( visitor != null )
                op.visit(visitor) ;
            after(op) ;
        }
    }
}
