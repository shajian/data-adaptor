package com.qianzhan.qichamao.graph;


/**
 * company, person and relation between them
 */
@ArangoGraphMeta(db = "company", graph = "cmap", edge = "cpr", froms = {"cp"}, tos = {"cp"})
@ArangoCollectionMeta(db = "company", collection = "cp", indices = {"name"})

@ArangoGraphMeta(env = 2, db = "business", graph = "graph", edge = "relation", froms = {"company", "person"}, tos = {"company"})
@ArangoCollectionMeta(env = 2, db = "business", collection = "company", indices = {"degree"})
@ArangoCollectionMeta(env = 2, db = "business", collection = "person", indices = {"name", "degree"})
public class ArangoBusiness {
}
