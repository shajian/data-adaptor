package com.qianzhan.qichamao.graph;

/**
 * company person identity
 * This represents manual intervene
 * The principles are:
 * 1.   take person-vertex as center, to which some related code-vertices are connected.
 *      person-vertex's key is composed with an unique sequence number and md5 of person name,
 *      so different person with same name will be distinguished by sequence number.
 * 2.   All code vertices that connect to the same person vertex(with same sequence number and same person name)
 *      means these code vertices are all related each other(white pair). If two code vertices connect to two
 *      different person vertices (with different sequence number but same person name) respectively, then it
 *      means the two code vertices are unrelated(black pair).
 * When adding a black/white pair, e.g. (person name, code1, code2, 0/1), where 0 denotes black while 1 denotes white.
 *      Firstly, you should search in ahead for the person name from ArangoDB, and if exists, some clusters will return.
 *      Remember, all codes in the same cluster means they are related each other, while two codes comes from different
 *      clusters means unrelated.
 *      1. For black, if code1 and code2 exist in the same cluster, then remove the two edges
 *          (person name -- code1, person name -- code2); otherwise jump to step 2.
 *      2. you(gay before the computer screen) decide which clusters should code1 and code2 be added in,
 *          if there is no existing cluster that code1 or code2 should be added in, then a new private cluster will
 *          be created, and we add two edges after. To here, black pair is handled.
 *      3. For white, if code1 and code2 exist in different clusters, merge the two person vertices(with same person
 *          name but different sequence number); if code1 and code2 already exist in the same cluster, nothing needs
 *          to do; if only code1 or only code2 exists, and add code2 or code1 into the same cluster; if both code1
 *          and code2 do not exist, and you(gay before computer screen) decide which cluster or a new cluster(if no
 *          existing cluster is appropriate) should they be added in.
 *
 */
@ArangoGraphMeta(db = "company", graph = "gcpid", edge = "cpidr", froms = {"cpid"}, tos = {"cpid"})
@ArangoCollectionMeta(db = "company", collection = "cpid", indices = {"name", "sq"})

@ArangoGraphMeta(env = 2, db = "intervene", graph = "graph", edge = "relation", froms = {"person"}, tos = {"company"})
@ArangoCollectionMeta(env = 2, db = "intervene", collection = "company", indices = {"sq"})
@ArangoCollectionMeta(env = 2, db = "intervene", collection = "person", indices = {"name", "sq"})
public class ArangoGraphCpId {

}
