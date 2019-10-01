package com.datastax.sparql.graph;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class GraphFactory {
  private GraphFactory() {
  }
  
  public static TinkerGraph createGraphOfTheGods() {
    TinkerGraph g = getTinkerGraphWithNumberManager();
    generateGraphOfTheGods(g);
    return g;
  }
  
  public static void generateGraphOfTheGods(final TinkerGraph g) {
    Vertex saturn = g.addVertex(T.id, 1, T.label, "titan", "name", "saturn", "age", 10000);
    Vertex sky = g.addVertex(T.id, 2, T.label, "location", "name", "sky");
    Vertex sea = g.addVertex(T.id, 3, T.label, "location", "name", "sea");
    Vertex jupiter = g.addVertex(T.id, 4, T.label, "god", "name", "jupiter", "age", 5000);
    Vertex neptune = g.addVertex(T.id, 5, T.label, "god", "name", "neptune", "age", 4500);
    Vertex hercules = g.addVertex(T.id, 6, T.label, "demigod", "name", "hercules", "age", 30);
    Vertex alcmene = g.addVertex(T.id, 7, T.label, "human", "name", "alcmene", "age", 45);
    Vertex pluto = g.addVertex(T.id, 8, T.label, "god", "name", "pluto", "age", 4000);
    Vertex nemean = g.addVertex(T.id, 9, T.label, "monster", "name", "nemean");
    Vertex hydra = g.addVertex(T.id, 10, T.label, "monster", "name", "hydra");
    Vertex cerberus = g.addVertex(T.id, 11, T.label, "monster", "name", "cerberus");
    Vertex tartarus = g.addVertex(T.id, 12, T.label, "location", "name", "tartarus");
    
    jupiter.addEdge("father", saturn, T.id, 101);
    jupiter.addEdge("lives", sky, T.id, 102, "reason", "loves fresh breezes");
    jupiter.addEdge("brother", neptune, T.id, 103);
    jupiter.addEdge("brother", pluto, T.id, 104);
    neptune.addEdge("lives", sea, T.id, 105, "reason", "loves waves");
    neptune.addEdge("brother", jupiter, T.id, 106);
    neptune.addEdge("brother", pluto, T.id, 107);
    hercules.addEdge("father", jupiter, T.id, 108);
    hercules.addEdge("mother", alcmene, T.id, 109);
    hercules.addEdge("battled", nemean, T.id, 110, "time", 1, "place", "38.1 23.7");
    hercules.addEdge("battled", hydra, T.id, 111, "time", 2, "place", "37.7 23.9");
    hercules.addEdge("battled", cerberus, T.id, 112, "time", 12, "place", "39 22");
    pluto.addEdge("brother", jupiter, T.id, 113);
    pluto.addEdge("brother", neptune, T.id, 114);
    pluto.addEdge("lives", tartarus, T.id, 115, "reason", "no fear of death");
    pluto.addEdge("pet", cerberus, T.id, 116);
    cerberus.addEdge("lives", tartarus, T.id, 117);
  }
  
  private static TinkerGraph getTinkerGraphWithNumberManager() {
    Configuration conf = getNumberIdManagerConfiguration();
    return TinkerGraph.open(conf);
  }
  
  private static Configuration getNumberIdManagerConfiguration() {
    Configuration conf = new BaseConfiguration();
    conf.setProperty("gremlin.tinkergraph.vertexIdManager", TinkerGraph.DefaultIdManager.INTEGER.name());
    conf.setProperty("gremlin.tinkergraph.edgeIdManager", TinkerGraph.DefaultIdManager.INTEGER.name());
    conf.setProperty("gremlin.tinkergraph.vertexPropertyIdManager", TinkerGraph.DefaultIdManager.LONG.name());
    return conf;
  }
  
}
