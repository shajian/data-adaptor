package com.qcm.task.maintask;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qcm.config.GlobalConfig;
import com.qcm.dal.RedisClient;
import com.qcm.dal.arangodb.ArangoBaseRepository;
import com.qcm.dal.arangodb.ArangoBusinessRepository;
import com.qcm.dal.arangodb.ArangoInterveneRepository;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.*;
import com.qcm.es.entity.EsUpdateLogEntity;
import com.qcm.es.repository.EsUpdateLogRepository;
import com.qcm.graph.*;
import com.qcm.task.specialtask.ComDtlTask;
import com.qcm.task.specialtask.BaseTask;
import com.qcm.task.specialtask.ComMemberTask;
import com.qcm.task.specialtask.ComShareHolderTask;
import com.qcm.util.DbConfigBus;
import com.qcm.util.MiscellanyUtil;
import com.qcm.util.parallel.Master;

import java.io.*;
import java.util.*;
import java.util.function.Function;

public class ArangoComTask extends com.qcm.task.maintask.BaseTask {
//    private static ArangoComClient client = ArangoComClient.getSingleton();

    private int path_thre;
    private int max_traverse_depth;
    public static int dist_step = 1000;
    @Deprecated
    private int subTableIndex;
    @Deprecated
    private List<String> subTableNames;
    // lp:0    sh:1    sm:1
    @Deprecated
    private int lp_sh_sm;


    public void insert(boolean overwrite) {
        insert_static(this.task, overwrite);
    }
    public void insert() {
        insert_static(this.task, true);
    }

    public static void insert_static(TaskType task, boolean overwrite) {
        try {
            ArangoWriter.insert(SharedData.getBatch(task), overwrite);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ArangoComTask() throws Exception {
        super("config/Task_Arango_Company.txt");

        checkpointName = config.getString("checkpoint_name");
        if (checkpointName == null) checkpointName = "data-adaptor.arango.company";
        dist_step = config.getInt("dist_step", 1000);
        ArangoBusinessRepository.singleton();       // preheat this object
    }



    protected void state1_pre() throws Exception {
        ArangoBusinessRepository.singleton().createGraph();
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(task));

        postHooks.add(() -> SharedData.closeBatch(task));
//        int chkpt = config.getInt("checkpoint", -1);
//        if (chkpt > 0) checkpoint = chkpt;
    }

    /**
     * this is the normal method/process: read OrgCompanyList,
     *  and then multi-threadly read legal person, share holder and senior member
     *  but, only insert/insert legal person, share holder and senior member into Arangodb.
     *
     *  checkpoint: 146551867
     * @return
     * @throws Exception
     */
    protected boolean state1_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

        com.qcm.task.specialtask.BaseTask.resetLatch(task, batch*3);
        int count = 0;
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
            String area = company.oc_area;
            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
            company.oc_name = company.oc_name.trim();
            if (filter_out(company.oc_name)) continue;

            SharedData.open(task);
            ComPack cp = SharedData.get(task);

            ArangoBusinessPack a_com = cp.arango;
            a_com.oc_code = company.oc_code;
            a_com.oc_area = company.oc_area;
            a_com.com = new ArangoBusinessCompany(company.oc_code, company.oc_name, company.oc_area).to();

            //
            pool.execute(new ComDtlTask(task));
            pool.execute(new ComMemberTask(task));
            pool.execute(new ComShareHolderTask(task));
//            pool.execute(new ComContactTask(task));

            SharedData.close(task);
            count++;
        }
        // wait for all sub-tasks finishing
        int diff = (batch-count)*3;
        while(BaseTask.getLatch(task).getCount() != diff) {
            Thread.sleep(30);
        }

        insert();
        return true;
    }

    protected void state3_pre() throws Exception {
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(task));

        postHooks.add(() -> SharedData.closeBatch(task));
    }

    protected boolean state3_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();

        Map<String, ArangoBusinessCompany> codes = new HashMap<>();
        for (OrgCompanyList company :
                companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            codes.put(company.oc_code, new ArangoBusinessCompany(company.oc_code, company.oc_name, company.oc_area));
        }
        // get all vertices via codes
        Collection<BaseDocument> docs = business.get(ArangoBusinessCompany.collection, codes.keySet());
        if (docs == null) return true;
        for (BaseDocument doc : docs) {
            codes.remove(doc.getKey());     // remove those had been saved in arangodb
        }
        if (codes.size() > 0) {             // those docs not existed in arangodb
            BaseTask.resetLatch(task, codes.size()*3);
            for (String code : codes.keySet()) {
                SharedData.open(task);
                ComPack cp = SharedData.get(task);
                ArangoBusinessPack a_com = cp.arango;
                a_com.com = codes.get(code).to();
                pool.execute(new ComDtlTask(task));
                pool.execute(new ComMemberTask(task));
                pool.execute(new ComShareHolderTask(task));
                SharedData.close(task);
            }
            while(BaseTask.getLatch(task).getCount() > 0) {
                Thread.sleep(30);
            }
            insert(false);
        }
        if (checkpoint>= 146407862) return false;
        return true;
    }


    // there are some companies that invest huge amount of child-companies, I take these companies(and their clusters)
    //  as dirty cases and skip to handle them temporarily. At last, I will specially handle them arbitrarily.
    private Set<String> dirtyHacks = new HashSet<String>();
    protected void state4_pre() throws Exception {
        path_thre = config.getInt("path_thre", 3);
        max_traverse_depth = config.getInt("max_traverse_depth", path_thre);
        System.out.println("max_traverse_depth: "+max_traverse_depth);
        if (config.getInt("use_redis", 0) == 0) {
            trivials = new HashSet<>();
        } else {
            int db = config.getInt("redis_temp_db", 0);
            if (db > 0) {
                redis_temp_db = db;
            } else if ((db= DbConfigBus.getDbConfig_i("redis.db.temp", 0)) > 0) {
                redis_temp_db = db;
            }
            trivials = RedisClient.smembers(trivial_key, redis_temp_db);
        }

        // load dirty hacked company codes
        try {
            File f = new File("config/Task_Arango_Company_DirtyHacks.txt");
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = null;
            while ((line = br.readLine()) != null) {
                dirtyHacks.add(line);
            }
            br.close();
            System.out.println(String.format("%d company codes loaded.", dirtyHacks.size()));
            for (String code :
                    dirtyHacks) {
                System.out.println(code);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * combine vertices with a same name
     * checkpoint: 146447862. Because some companies are not saved in Redis, so many vertices is not a completed company
     *  vertices.
     * @return
     * @throws Exception
     */
    protected boolean state4_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

//        List<String> toRemoved = new ArrayList<>();
        HashSet<String> temp_rems = new HashSet<>();
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
            String area = company.oc_area;
            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
            company.oc_name = company.oc_name.trim();
            if (filter_out(company.oc_name)) continue;

            // delete edge of type 4 -> share the same contacts
            // early we think if two companies share a same contact, there is some relation between them;
            //  however, this guess is not correct at most time.
//            toRemoved.addAll(getEdgeByContact(company.oc_code));  // remove once a time
//            codes.add(company.oc_code);                         // collect all and then remove them once a batch

            if (trivials.contains(company.oc_code)) {
                trivials.remove(company.oc_code);
                temp_rems.add(company.oc_code);
                continue;
            }
            combine(company.oc_code, company.oc_id);
        }

        if (temp_rems.size() > 0) {
            String[] rems = temp_rems.toArray(new String[1]);
            RedisClient.srem(redis_temp_db, trivial_key, rems);
        }

//        toRemoved.addAll(getEdgeByContact_Parallel(codes));     // collect all edges
//
//        if (toRemoved.size() > 0) {
//            System.out.println(String.format("%d edges by contact will be removed.", toRemoved.size()));
//            client.bulkDelete_ED(toRemoved);
//        }
//        if (checkpoint>= 146447067) return false;
        return true;
    }


    /**
     * collect all edges with type `contact` according to a group of code vertices
     * collecting is executed in parallel
     * @param codes
     * @return
     */
    private Collection<String> getEdgeByContact_Parallel(List<String> codes) {
        int workers = 10;
        Function<String, List<String>> collector = code -> getEdgeByContact(code);
        LinkedList<String> queue = new LinkedList<>();
        queue.addAll(codes);
        Master<String, List<String>> master = new Master<String, List<String>>(workers, queue, collector);
        List<List<String>> results = master.start();
        Set<String> keys = new HashSet<>();
        for (List<String> list: results) {
            for (String result : list) {
                keys.add(result);
            }
        }
//
        return keys;
    }

    /**
     * collect all edges with type `contact` according to a code vertex
     * @param oc_code
     * @return
     */
    private List<String> getEdgeByContact(String oc_code) {
        List<String> toRemoved = new ArrayList<>();
        try {
            ArangoBusinessRepository cp = ArangoBusinessRepository.singleton();
            String id = cp.getGraphMeta().froms()[0] + "/" + oc_code;
            List<BaseEdgeDocument> edges = cp.neighbours(id);

            if (!MiscellanyUtil.isArrayEmpty(edges)) {
                for (BaseEdgeDocument edge : edges) {
                    Map<String, Object> props = edge.getProperties();
                    Object tp = props.get("type");
                    if (tp != null) {
                        long tp_l = (Long) tp;
                        if (tp_l == 4) {
                            toRemoved.add(edge.getKey());
                        }
                    }
                }
            }
        } catch (Exception e) {

        }
        return toRemoved;
    }

    // it is used to accelerate vertex merging for those clusters with large size
    private Set<String> trivials;
    private int redis_temp_db = 9;
    private String trivial_key = "graph-trivials";  // cache these trivials into Redis


//    {{
//        add("MA1MCM899");
//    }};

    /**
     * before combination, do some simple filtering
     * 1. if this company has no person vertex connected
     * 2. if this company connects to a vertex that is too `dirty`(degree > 1E5)
     * @param code
     * @return if false, subsequent combination is not needed
     * @throws Exception
     */
    private boolean combine_SimpleFilter(String code) throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();

        List<BaseEdgeDocument> froms = business.neighbours(ArangoBusinessCompany.toId(code), ArangoBaseRepository.InOut.in);

        boolean noPerson = true;
        boolean dirty = false;
        for (BaseEdgeDocument from : froms) {
            String fromId = from.getId();
            if (fromId.startsWith(ArangoBusinessPerson.collection)) noPerson = false;
            if (dirtyHacks.contains(from.getKey())) dirty = true;
        }
        if (noPerson || dirty) return false;
        return true;
    }

    /**
     *
     * @param vs all vertices in a cluster
     * @return outliers: all person typed vertices that connected by company typed vertices
     *          but are out of this cluster
     * @throws Exception
     */
    private Map<String, Set<String>> outlierPersonNeighbourGroupsByCompany(List<BaseDocument> vs) throws Exception {
        // key: company vertex id, value: its all neighbours
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        Map<String, Set<String>> outlierPersonNeighbourGroups = new HashMap<>();
        List<String> toIds = new ArrayList<>();     // collect all company-vertices
        Set<String> personVertexIds = new HashSet<>();
        for (BaseDocument v : vs) {
            if (v.getId().startsWith(ArangoBusinessPerson.collection)) personVertexIds.add(v.getId());
        }

        for (BaseDocument v : vs) {
            if (v == null) continue;
            if (v.getKey().length() == 9) toIds.add(v.getId()); // collect company typed vertices

            if (toIds.size() > 256) {
                List<BaseEdgeDocument> edges = business.searchByTos(toIds);
                for (BaseEdgeDocument edge : edges) {
                    if (edge == null) continue;
                    Set<String> group = outlierPersonNeighbourGroups.get(edge.getTo());
                    if (group == null) {            // even a company vertex has no outlier-person-vertex connected,
                        group = new HashSet<>();    //  we still build a group for it
                        outlierPersonNeighbourGroups.put(edge.getTo(), group);
                    }

                    String from = edge.getFrom();
                    if (from.startsWith(ArangoBusinessCompany.collection)) continue;
                    if (personVertexIds.contains(from)) continue;       // collect only outliers
                    group.add(from);            // collect its neighbours
                }
                toIds.clear();
            }
        }
        if (toIds.size() > 0) {
            List<BaseEdgeDocument> edges = business.searchByTos(toIds);
            for (BaseEdgeDocument edge : edges) {
                if (edge == null) continue;

                Set<String> group = outlierPersonNeighbourGroups.get(edge.getTo());
                if (group == null) {
                    group = new HashSet<>();
                    outlierPersonNeighbourGroups.put(edge.getTo(), group);
                }
                String from = edge.getFrom();
                if (from.startsWith(ArangoBusinessCompany.collection)) continue;
                if (personVertexIds.contains(from)) continue;       // collect only outliers

                group.add(from);
            }
        }
        return outlierPersonNeighbourGroups;
    }

    /**
     *
     * @param record_id id in database of current company that is now being handled with
     * @param outlierPersonNeighbourGroups
     */
    private void addTrivials(int record_id, Map<String, Set<String>> outlierPersonNeighbourGroups) {
        // check out those companies whose all neighbours are already included in this current
        //  connected graph represented by `vertices`.
        //  Those companies have no vertices to be merged after current merging, so in subsequent iterations,
        //  if meet those companies again, we can skip them directly for efficiency.
        StringBuilder existing_sql = new StringBuilder();
        HashSet<String> temp_add = new HashSet<>();
        HashSet<String> temp_rem = new HashSet<>();
        for (String key : outlierPersonNeighbourGroups.keySet()) {
            if (outlierPersonNeighbourGroups.get(key).size() == 0) {
                String trivial = key.split("/")[1];
                if (existing_sql.length() == 0) {
                    existing_sql.append("select oc_id, oc_code from OrgCompanyList where oc_code in ('")
                            .append(trivial).append("'");
                } else {
                    existing_sql.append(", '").append(trivial).append("'");
                }

                if (existing_sql.length() > 5000) {
                    existing_sql.append(")");
                    List<Map<String, Object>> docs = MybatisClient.selectMany(existing_sql.toString());

                    for (Map<String, Object> doc : docs) {
                        Integer oc_id = (Integer) doc.get("oc_id");
                        String oc_code = (String) doc.get("oc_code");
                        if (oc_id != null) {
                            int id = oc_id;
                            if (id < record_id) {
                                trivials.remove(oc_code);   // company with id `id` had been handled over early.
                                temp_rem.add(oc_code);
                            } else {
                                trivials.add(oc_code);      // add company in trivials that in future when we meet
                                temp_add.add(oc_code);      //  it again, we will skip it instead of do combining
                            }
                        }
                    }
                    existing_sql.delete(0, existing_sql.length());
                }
            }
        }
        if (existing_sql.length() > 0) {
            existing_sql.append(")");
            List<Map<String, Object>> docs = MybatisClient.selectMany(existing_sql.toString());
            for (Map<String, Object> doc : docs) {
                Integer oc_id = (Integer) doc.get("oc_id");
                String oc_code = (String) doc.get("oc_code");
                if (oc_id != null) {
                    int id = oc_id;
                    if (id < record_id) {
                        trivials.remove(oc_code);
                        temp_rem.add(oc_code);
                    } else {
                        trivials.add(oc_code);
                        temp_add.add(oc_code);
                    }
                }
            }
        }
        if (temp_add.size() > 0) {
            String[] adds = temp_add.toArray(new String[1]);
            RedisClient.sadd(redis_temp_db, trivial_key, adds);
        }
        if (temp_rem.size() > 0) {
            String[] rems = temp_rem.toArray(new String[1]);
            RedisClient.srem(redis_temp_db, trivial_key, rems);
        }
    }

    private void combine(String code, int record_id) throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        List<BaseDocument> vertices = business.cluster(ArangoBusinessCompany.toId(code), max_traverse_depth);
        if (MiscellanyUtil.isArrayEmpty(vertices)) return;
        int clusterSize = vertices.size();


        if (record_id > 0) {
            if (clusterSize > 8000) {
                // key: company vertex id, value: its all neighbours.
                // Statistics all companies with their own neighbours in this cluster
                Map<String, Set<String>> outlierPersonNeighbourGroups = outlierPersonNeighbourGroupsByCompany(vertices);
                addTrivials(record_id, outlierPersonNeighbourGroups);
            }
        }

        // after get all _from of `code` and directly connected companies of code
        // group vertex ids by personal name(actually name_md5)
        // key: name_md5, value: person typed vertices share the same name
        Map<String, List<BaseDocument>> groups = new HashMap<>(); // keep all natural persons and unknown companies

        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;

            String key = vertex.getKey();   //.split("/")[1];
            if (key.length() <= 9) continue;    // company-type node, do not need to merge vertex, skip
            if (GlobalConfig.getEnv() == 2 && vertex.getId().startsWith(ArangoBusinessCompany.collection)) continue;

            String name_md5 = key.substring(9);
            List<BaseDocument> group = groups.get(name_md5);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(name_md5, group);
            }
            group.add(vertex);  // person vertices which share a same person name
        }



        // key: oc_code, value: it's all person vertex's id
        Map<String, Set<String>> intervenes = new HashMap<>();

        for (String name_md5 : groups.keySet()) {
            List<BaseDocument> group = groups.get(name_md5);
            if (group.size() == 1) continue;    // no more than 2 person shares the same name, skip

            // can we directly combine a group since they all share a same person name?
            // YES, we can because here the max depth of traversing is 2 which has met the lower limit,
            //  however, it deserves to note the existence of intervene.
            //  To this end, the intervene hasn't been started for using, so just combine them directly temporarily
            String new_from_id = null;
            List<String> old_from_ids = new ArrayList<>();
            for (BaseDocument doc : group) {
                if (new_from_id == null) new_from_id = doc.getId();
                else old_from_ids.add(doc.getId());
            }

            business.merge(old_from_ids, new_from_id, false);
        }
    }
    /**
     * combine two vertices with a same name
     * @param code oc_code of a vertex in a chain
     */
    @Deprecated
    private void combine_1(String code, int idx) throws Exception {
        ArangoInterveneRepository intervene = ArangoInterveneRepository.singleton();
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        // because there may be many vertices connect to the code vertex, so we set max_deep=2 here
        List<BaseEdgeDocument> fromEdges = business.neighbours(ArangoBusinessCompany.toId(code), ArangoBaseRepository.InOut.in);
        boolean noPersonFlag = true;        // company that has no person vertex connected.
        boolean dirty = false;
        for (BaseEdgeDocument edge:
                fromEdges) {
            if (edge.getFrom().startsWith(ArangoBusinessPerson.collection)) {
                noPersonFlag = false;
            }
            String key = edge.getFrom().substring(ArangoBusinessCompany.collection.length()+1);
            if (dirtyHacks.contains(key)) {
                dirty = true;   // if check out a dirty cluster center, then mark it and return directly.
            }
        }
        if (noPersonFlag || dirty) {
//            System.out.println(String.format(
//                    "company %s is skipped. dirty: %b, noPersonFlag: %b", code, dirty, noPersonFlag));
            return;
        }

        List<BaseDocument> vertices = business.cluster(ArangoBusinessCompany.toId(code), max_traverse_depth);
        if (MiscellanyUtil.isArrayEmpty(vertices)) return;
        int graphSize = vertices.size();

        // key: company vertex id, value: its all neighbours
        Map<String, Set<String>> companyWithNeighbours = new HashMap<>();
        if (graphSize > 8000) {
            List<String> toIds = new ArrayList<>();     // collect all company-vertices
            for (BaseDocument vertex : vertices) {      // traverse all vertices on this graph
                if (vertex == null) continue;
                if (vertex.getKey().length() == 9) {    // company
                    toIds.add(vertex.getId());
                }

                if (toIds.size() >= 256) {              // if company-vertices is too many, search them batch by batch
                    List<BaseEdgeDocument> edges = business.searchByTos(toIds);
                    for (BaseEdgeDocument edge : edges) {
                        if (edge == null) continue;
                        String from = edge.getFrom();

                        Set<String> group = companyWithNeighbours.get(edge.getTo());
                        if (group == null) {
                            group = new HashSet<>();
                            companyWithNeighbours.put(edge.getTo(), group);
                        }
                        group.add(from);            // collect its neighbours
                    }
                    toIds.clear();
                }
            }
            if (toIds.size() > 0) {                 // do not forget the tail batch
                List<BaseEdgeDocument> edges = business.searchByTos(toIds);
                for (BaseEdgeDocument edge : edges) {
                    if (edge == null) continue;
                    String from = edge.getFrom();

                    Set<String> group = companyWithNeighbours.get(edge.getTo());
                    if (group == null) {
                        group = new HashSet<>();
                        companyWithNeighbours.put(edge.getTo(), group);
                    }
                    group.add(from);
                }
            }
        }

        // after get all _from of `code` and directly connected companies of code
        // group vertex ids by personal name(actually name_md5)
        // key: name_md5, value: person typed vertices share the same name
        Map<String, List<BaseDocument>> groups = new HashMap<>(); // keep all natural persons and unknown companies
        int personNum = 0;      // number of all person vertices

        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;


            // exclude the vertex that will be handled later
            for (Set<String> neighbours : companyWithNeighbours.values()) {
                neighbours.remove(vertex.getId());  //
            }

            String key = vertex.getKey();   //.split("/")[1];
            if (key.length() <= 9) continue;    // company-type node, do not need to merge vertex, skip
            if (GlobalConfig.getEnv() == 2 && vertex.getId().startsWith(ArangoBusinessCompany.collection)) continue;



            personNum++;
            String name_md5 = key.substring(9);
            List<BaseDocument> group = groups.get(name_md5);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(name_md5, group);
            }
            group.add(vertex);  // person vertices which share a same person name
        }

        // check out those companies whose all neighbours are already included in this current
        //  connected graph represented by `vertices`.
        //  Those companies have no vertices to be merged after current merging
        StringBuilder existing_sql = new StringBuilder();
        boolean finished = false;
        HashSet<String> temp_add = new HashSet<>();
        HashSet<String> temp_rem = new HashSet<>();
        for (String key : companyWithNeighbours.keySet()) {
            if (companyWithNeighbours.get(key).size() == 0) {
                String trivial = key.split("/")[1];
                if (existing_sql.length() == 0) {
                    existing_sql.append("select oc_id, oc_code from OrgCompanyList where oc_code in ('")
                            .append(trivial).append("'");
                } else {
                    existing_sql.append(", '").append(trivial).append("'");
                }

                if (existing_sql.length() > 5000) {
                    existing_sql.append(")");
                    List<Map<String, Object>> docs = MybatisClient.selectMany(existing_sql.toString());

                    for (Map<String, Object> doc : docs) {
                        Integer oc_id = (Integer) doc.get("oc_id");
                        String oc_code = (String) doc.get("oc_code");
                        if (oc_id != null) {
                            int id = oc_id;
                            if (id < idx) {
                                trivials.remove(oc_code);
                                temp_rem.add(oc_code);
                            } else if (id == idx) {
                                finished = true;
                                trivials.remove(oc_code);
                                temp_rem.add(oc_code);
                            } else {
                                trivials.add(oc_code);
                                temp_add.add(oc_code);
                            }
                        }
                    }
                    existing_sql.delete(0, existing_sql.length());
                }
            }
        }
        if (existing_sql.length() > 0) {
            existing_sql.append(")");
            List<Map<String, Object>> docs = MybatisClient.selectMany(existing_sql.toString());
            for (Map<String, Object> doc : docs) {
                Integer oc_id = (Integer) doc.get("oc_id");
                String oc_code = (String) doc.get("oc_code");
                if (oc_id != null) {
                    int id = oc_id;
                    if (id < idx) {
                        trivials.remove(oc_code);
                        temp_rem.add(oc_code);
                    } else if (id == idx) {
                        finished = true;
                        trivials.remove(oc_code);
                        temp_rem.add(oc_code);
                    } else {
                        trivials.add(oc_code);
                        temp_add.add(oc_code);
                    }
                }
            }
        }
        if (temp_add.size() > 0) {
            String[] adds = temp_add.toArray(new String[1]);
            RedisClient.sadd(redis_temp_db, trivial_key, adds);
        }
        if (temp_rem.size() > 0) {
            String[] rems = temp_rem.toArray(new String[1]);
            RedisClient.srem(redis_temp_db, trivial_key, rems);
        }
        if (finished) return;



        // key: oc_code, value: it's all person vertex's id
        Map<String, Set<String>> intervenes = new HashMap<>();

        for (String name_md5 : groups.keySet()) {
            List<BaseDocument> group = groups.get(name_md5);
            if (group.size() == 1) continue;    // no more than 2 person shares the same name, skip

            // can we directly combine a group since they all share a same person name?
            // YES, we can because here the max depth of traversing is 2 which has met the lower limit,
            //  however, it deserves to note the existence of intervene.
            //  To this end, the intervene hasn't been started for using, so just combine them directly temporarily
            String new_from_id = null;
            List<String> old_from_ids = new ArrayList<>();
            for (BaseDocument doc : group) {
                if (new_from_id == null) new_from_id = doc.getId();
                else old_from_ids.add(doc.getId());
            }

            business.merge(old_from_ids, new_from_id, false);
        }
    }

    /**
     * update vertex degree
     * only person vertices are concerned at current time.
     */
    private boolean updateDegree() throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        List<BaseDocument> documents = business.scan(ArangoBusinessPerson.collection, checkpoint, batch);

        List<String> ids = new ArrayList<>();           // stores person vertices
        Map<String, Integer> map = new HashMap<>();     //
        for (BaseDocument document : documents) {
            if (document == null) continue;
            if (GlobalConfig.getEnv() == 1) {
                Object type = document.getAttribute("type");
                if (type != null) {
                    Long type_l = (Long) type;
                    if (type_l == 1) continue;
                }
            }
            ids.add(document.getId());
            map.put(document.getId(), 0);
        }
        List<BaseEdgeDocument> fromEdges = business.searchByFroms(ids);
        if (fromEdges == null) return documents.size() > 0;

        for (BaseEdgeDocument edge : fromEdges) {
            if (edge == null) continue;
            String from = edge.getFrom();   // parallel edges should be counted repeatedly.
            Integer count = map.get(from);
            if (count == null) map.put(from, 1);
            else map.put(from, count+1);
        }
        StringBuilder sb = new StringBuilder();

        for (String id : map.keySet()) {
            if (sb.length() == 0) {
                sb.append("LET persons = [{_key: '");
                sb.append(id.split("/")[1]).append("', c: ").append(map.get(id)).append("}");
            } else {
                sb.append(", {_key: '").append(id.split("/")[1]).append("', c: ").append(map.get(id)).append("}");
            }
        }
        sb.append("]\nFOR v in persons UPDATE v WITH {degree: v.c} IN ")
                .append(ArangoBusinessPerson.collection).append(" RETURN NEW");
        String aql = sb.toString();
        System.out.println(aql);
        List<BaseDocument> docs = business.execute(aql);

        for (BaseDocument doc : docs) {
            String name = (String) doc.getAttribute("name");
            Long degree = (Long) doc.getAttribute("degree");
            long d = degree == null ? -1 : (long) degree;
            System.out.println(String.format("document %s-%s has degree %d", doc.getKey(), name, d));
        }
        checkpoint += batch;
        return documents.size() > 0;
    }

    protected void state2_pre() throws Exception {
        // todo update ArangoDB `company-person-relation`
    }

    /**
     * update all data in ArangoDB day by day.
     * This state is corresponding to state1 which usually means writing data into
     * database firstly.
     * @return
     * @throws Exception
     */
    protected boolean state2_inner() throws Exception {
        // todo update ArangoDB `company-person-relation`
        // pop data from synchronized table
        List<OrgCompanyUpdateMeta> metas = MybatisClient.getCompanyUpdateMeta(checkpoint, batch);

        if (metas.size() == 0) return false;

        Set<String> uniques = new HashSet<>();
        List<OrgCompanyUpdateMeta> newMetas = new ArrayList<>();
        List<EsUpdateLogEntity> entities = new ArrayList<>();
        for (OrgCompanyUpdateMeta meta : metas) {       // remove redundant records that will be updated
            String u = meta.table_name+meta.field_names+meta.field_values;
            if (!uniques.contains(u)) {
                uniques.add(u);
                newMetas.add(meta);
            }
            entities.add(ComUtil.transfer(task, meta));
        }
        // update `name` and `area`(via overwriting them directly)
        metas = state2_inner_info_overwrite(newMetas);

        // collect all other fields that influence corresponding relation edges
        Map<String, ArangoBusinessCompanyDiff> diffs = state2_inner_collect_new_LPSHSM(metas);

        // collect old infos
        state2_inner_fill_old_LPSHSM(diffs);

        // do differentiation
        for (String code : diffs.keySet()) {
            state2_inner_update_LPSHSM(diffs.get(code));
        }

        if (entities.size() > 0)
            EsUpdateLogRepository.singleton().index(entities);
        return true;
    }

/**
 * // search the legal person of this company from arangodb
 *                     String md5 = Cryptor.md5(c.od_faRen);
 *                     List<BaseEdgeDocument> edges = business.neighbours(ArangoBusinessCompany.toId(c.od_oc_code), ArangoBaseRepository.InOut.in);
 *                     for (BaseEdgeDocument edge : edges) {
 *                         Long type = (Long)edge.getAttribute("type");
 *                         if (type != null && type == 1) {        // legal person, we suppose each company has a single legal person
 *                             if (edge.getFrom().endsWith(md5)) {
 *                                 // legal person is not changed for this company, nothing should be done
 *                             } else {
 *                                 BaseDocument oldLP = business.get(edge.getFrom());
 *                                 if (c.od_faRen.equals(oldLP.getAttribute("name"))) {
 *                                     // keep unchanged
 *                                 } else {
 *                                     // if `from` vertex only connects to this company, delete it (and the relation edge) directly
 *                                     // else, delete the relation edge, and reconnect this company to a correct vertex
 *
 *                                     List<BaseEdgeDocument> ns = business.neighbours(oldLP.getId(), ArangoBaseRepository.InOut.out);
 *                                     if (ns.size() < 2) {
 *                                         business.delete(oldLP.getId());
 * //                                    business.delete(edge.getId());    // edge will be automatically deleted by arangodb?
 *                                     } else {
 *                                         // degree will not be updated here
 * //                                        int degree = ns.size() - 1;
 * //                                        business.execute("UPDATE {_key: '" + oldLP.getKey() + "'} WITH {degree: "
 * //                                                + degree + "} IN " + oldLP.getId().split("/")[0]);
 *                                         business.delete(edge.getId());
 *                                     }
 *                                     BaseDocument newLP = ComDtlTask.fromLegalPerson(c.od_oc_code, c.od_faRen);
 *
 *                                     boolean combined = false;
 *                                     if (newLP.getId().startsWith(ArangoBusinessPerson.collection)) {
 *                                         // combine vertex for this legal person
 *                                         String thiis = (String) newLP.getAttribute("name");
 *                                         List<BaseDocument> vertices = business.cluster(ArangoBusinessCompany.toId(c.od_oc_code), max_traverse_depth);
 *                                         for (BaseDocument vertex : vertices) {
 *                                             String other = (String) vertex.getAttribute("name");
 *                                             if (other.equals(thiis)) {
 *                                                 combined = true;
 *                                                 newLP = vertex;
 *                                                 break;
 *                                             }
 *                                         }
 *                                     }
 *
 *                                     if (!combined) {
 *                                         BaseDocument oldNewLP = business.get(newLP.getId());
 *                                         if (oldNewLP == null) {
 *                                             business.insert(newLP);
 *                                         }
 *                                     }
 *                                     business.insert(new ArangoBusinessRelation(newLP.getId(), ArangoBusinessCompany.toId(c.od_oc_code),
 *                                             "lp"+c.od_oc_code+newLP.getKey(), 1).to());
 *                                 }
 *                             }
 *                         }
 *                     }
 */

/**
 * update some infos that can be directly overwrited
     * e.g. `name` and `area` field of a vertex
     */
    private List<OrgCompanyUpdateMeta> state2_inner_info_overwrite(List<OrgCompanyUpdateMeta> metas) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<OrgCompanyUpdateMeta> newMetas = new ArrayList<>();
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        for (OrgCompanyUpdateMeta meta : metas) {
            if ("OrgCompanyList".equals(meta.table_name)) {
                if ("oc_code".equals(meta.field_names)) {
                    OrgCompanyList c = MybatisClient.getCompany(meta.field_values);
                    // only area and name should be checked
                    if (sb.length() == 0) {
                        sb.append("LET entities = [{_key: '");
                        sb.append(c.oc_code).append("', a: '").append(c.oc_area).append("', n: '")
                                .append(c.oc_name).append("'}");
                    } else {
                        sb.append(", {_key: '").append(c.oc_code).append("', a: '").append(c.oc_area)
                                .append("', n: '").append(c.oc_name).append("'}");
                    }
                }
            } else {
                newMetas.add(meta);
            }
        }
        if (sb.length() > 0) {
            sb.append("]\nFOR v in entities UPDATE v WITH {area: v.a, name: v.n} IN ")
                    .append(ArangoBusinessCompany.collection).append(" RETURN NEW");
            String aql = sb.toString();
            try {
                business.execute(aql);
            } catch (Exception e) {
                // todo log
            }
        }
        return newMetas;
    }

    private Map<String, ArangoBusinessCompanyDiff> state2_inner_collect_new_LPSHSM(List<OrgCompanyUpdateMeta> metas) {
        Map<String, ArangoBusinessCompanyDiff> updateInfos = new HashMap<>();
        for (OrgCompanyUpdateMeta meta : metas) {
            String code = meta.field_values;
            ArangoBusinessCompanyDiff info = updateInfos.get(code);
            if (info == null) {
                info = new ArangoBusinessCompanyDiff();
                info.code = code;
                updateInfos.put(code, info);
            }

            if (meta.table_name == "OrgCompanyDtl") {
                OrgCompanyDtl c = MybatisClient.getCompanyDtl(meta.field_values);
                info.newLP = new ArangoBusinessCompanyLPInfo(c.od_faRen.trim());
            } else if (meta.table_name.startsWith("OrgCompanyGsxtDtlMgr_")) {
                List<OrgCompanyDtlMgr> mgrs = MybatisClient.getCompanyMembersGsxt(meta.field_values, meta.table_name.split("_")[1]);
                if (info.newSMs == null) info.newSMs = new HashMap<>();
                for (OrgCompanyDtlMgr m : mgrs) {
                    if (MiscellanyUtil.isBlank(m.om_name) || m.om_status == 4) continue;
                    m.om_name = m.om_name.trim();
                    ArangoBusinessCompanySMInfo sm = info.newSMs.get(m.om_name);
                    if (sm == null) {
                        sm = new ArangoBusinessCompanySMInfo(m.om_name);
                        info.newSMs.put(m.om_name, sm);
                    }
                    sm.occupations.add(m.om_position);
                }
            } else if (meta.table_name.startsWith("OrgCompanyGsxtDtlGD_")) {
                List<OrgCompanyGsxtDtlGD> gds = MybatisClient.getCompanyGDsGsxt(meta.field_values, meta.table_name.split("_")[1]);
                if (info.newSHs == null) info.newSHs = new HashMap<>();
                for (OrgCompanyGsxtDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name) || gd.og_status == 4) continue;
                    gd.og_name = gd.og_name.trim();
                    ArangoBusinessCompanySHInfo sh = info.newSHs.get(gd.og_name);
                    if (sh == null) {
                        sh = new ArangoBusinessCompanySHInfo(gd.og_name);
                        info.newSHs.put(gd.og_name, sh);
                    }
                    sh.money += gd.og_subscribeAccount;
                }
            } else if (meta.table_name.equals("OrgCompanyDtlGD")) {
                List<OrgCompanyDtlGD> gds = MybatisClient.getCompanyGDs(meta.field_values);
                if (info.newSHs == null) info.newSHs = new HashMap<>();
                for (OrgCompanyDtlGD gd : gds) {
                    if (MiscellanyUtil.isBlank(gd.og_name)) continue;
                    gd.og_name = gd.og_name.trim();
                    ArangoBusinessCompanySHInfo sh = info.newSHs.get(gd.og_name);
                    if (sh == null) {
                        sh = new ArangoBusinessCompanySHInfo(gd.og_name);
                        info.newSHs.put(gd.og_name, sh);
                    }
                    sh.money += gd.og_money;
                }
            } else if (meta.table_name.equals("OrgCompanyDtlMgr")) {
                List<OrgCompanyDtlMgr> mgrs = MybatisClient.getCompanyMembers(meta.field_values);
                if (info.newSMs == null) info.newSMs = new HashMap<>();
                for (OrgCompanyDtlMgr m : mgrs) {
                    if (MiscellanyUtil.isBlank(m.om_name) || m.om_status == 4) continue;
                    m.om_name = m.om_name.trim();
                    ArangoBusinessCompanySMInfo sm = info.newSMs.get(m.om_name);
                    if (sm == null) {
                        sm = new ArangoBusinessCompanySMInfo(m.om_name);
                        info.newSMs.put(m.om_name, sm);
                    }
                    sm.occupations.add(m.om_position);
                }
            }
        }
        return updateInfos;
    }

    private void state2_inner_fill_old_LPSHSM(Map<String, ArangoBusinessCompanyDiff> infos) throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        for (String code : infos.keySet()) {
            List<ArangoRay> relations = business.oneStepRelation(ArangoBusinessCompany.toId(code), ArangoBaseRepository.InOut.in);
            ArangoBusinessCompanyDiff info = infos.get(code);
            for (ArangoRay r : relations) {
                BaseEdgeDocument e = r.edge;
                assert e.getTo() == ArangoBusinessCompany.toId(code) : String.format("wrong edge: %s -> %s", e.getFrom(), e.getTo());
                Long type = (Long) e.getAttribute("type");
                assert type != null : String.format("edge %s has unknown type", e.getId());
                String name = (String) e.getAttribute("name");
                if (type == 1) {
                    info.oldLP = new ArangoBusinessCompanyLPInfo(name, r.vertex.getId(), e.getId());
                } else if (type == 2) { // share holder
                    ArangoBusinessCompanySHInfo sh = new ArangoBusinessCompanySHInfo(name, r.vertex.getId(), e.getId());
                    sh.money = (Double) e.getAttribute("money");
                    info.oldSHs.put(name, sh);
                } else if (type == 3) {
                    ArangoBusinessCompanySMInfo sm = new ArangoBusinessCompanySMInfo(name, r.vertex.getId(), e.getId());
                    sm.occupations.add((String) e.getAttribute("position"));
                    info.oldSMs.put(name, sm);
                }
            }
        }
    }

    private void state2_inner_update_LPSHSM(ArangoBusinessCompanyDiff diff) throws Exception {
        diff.diff();
        // save to arangodb.
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        Map<String, List<String>> groups = diff.groupByCollection(diff.removedVertices);
        for (String collection : groups.keySet()) {
            List<String> keys = groups.get(collection);
            business.delete(collection, keys);
        }
        if (diff.removedEdges.size() > 0)
            business.delete(ArangoBusinessRelation.collection, diff.removedEdges);
        if (diff.updatedEdges.size() > 0)
            business.insert(ArangoBusinessRelation.collection, diff.updatedEdges, true);
        List<BaseDocument> cs = new ArrayList<>();
        List<BaseDocument> ps = new ArrayList<>();
        for (String n : diff.insertedVertices.keySet()) {
            BaseDocument d = diff.insertedVertices.get(n);
            if (d.getId().startsWith(ArangoBusinessCompany.collection)) cs.add(d);
            else ps.add(d);
        }
        if (!cs.isEmpty()) business.insert(ArangoBusinessCompany.collection, cs, true);
        if (!ps.isEmpty()) business.insert(ArangoBusinessPerson.collection, ps, true);
        if (diff.insertedEdges.size() > 0)
            business.insert(ArangoBusinessRelation.collection, diff.insertedEdges, true);

        // combine person vertices
        if (!ps.isEmpty()) {
            combine(diff.code, -1);
        }
    }


    @Override
    protected void state6_pre() throws Exception {
    }

    /**
     * update degree of vertices
     * @return
     * @throws Exception
     */
    protected boolean state6_inner() throws Exception {
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
//        int company_vertex_id_length = ArangoBusinessCompany.collection.length()+10;

        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

        // key: vertex key, value: vertex out-in degree
        Map<String, Integer> pmap = new HashMap<>();     //
        Map<String, Integer> cmap = new HashMap<>();
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
            String area = company.oc_area;
            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
            company.oc_name = company.oc_name.trim();
            if (filter_out(company.oc_name)) continue;

            cmap.put(ArangoBusinessCompany.collection+"/"+company.oc_code, 0);
        }
        if (cmap.size() > 0) {
            // according to company nodes, searching all their neighbours(include person nodes)
            List<BaseEdgeDocument> edges = business.searchByTos(cmap.keySet());
            if (edges == null) return true;

            for (BaseEdgeDocument edge : edges) {
                String id = edge.getFrom();
                if (id.startsWith(ArangoBusinessCompany.collection)) {
                    if (!cmap.containsKey(id)) {
                        cmap.put(id, 0);
                    }
                } else {
                    if (!pmap.containsKey(id)) {
                        pmap.put(id, 0);
                    }
                }
            }

            updateDegreeInner(cmap);
            updateDegreeInner(pmap);
        }

        if (checkpoint>= 146407862) return false;
        return true;
    }

    private void updateDegreeInner(Map<String, Integer> map) throws Exception {
        if (map == null || map.size() == 0) return;
        String fid = map.keySet().iterator().next();
        ArangoBaseRepository.InOut io = ArangoBaseRepository.InOut.out;      // 2->_from; 1->_to; 3->both from and to
        String collection = ArangoBusinessPerson.collection;
        if (fid.startsWith(ArangoBusinessCompany.collection)){
            io = ArangoBaseRepository.InOut.both;
            collection = ArangoBusinessCompany.collection;
        }
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        Set<String> keys = new HashSet<>();
        for (String key :
                map.keySet()) {
            keys.add(key.split("/")[1]);
        }

        // removed those vertices whose degree had been updated
        Collection<BaseDocument> vertices = business.get(collection, keys);
        if (vertices == null) return;
        for (BaseDocument vertex : vertices) {
            Long degree = (Long) vertex.getAttribute("degree");
            if (degree != null && degree > 0) {     // removed those vertices whose degree had been updated
                String id = vertex.getId();
                map.remove(id);
            }
        }

        // update degree
        List<BaseEdgeDocument> edges = business.searchByEnds(map.keySet(), io);
        if (edges == null) return;
        for (BaseEdgeDocument edge : edges) {
            if (edge == null) continue;
            String from = edge.getFrom();   // parallel edges should be counted repeatedly.
            String to = edge.getTo();
            Integer count = map.get(from);
            if (count != null) {
                map.put(from, count+1);
            }
            count = map.get(to);
            if (count != null) {
                map.put(to, count+1);
            }
        }

        // construct AQL and update back to database
        StringBuilder sb = new StringBuilder();
        for (String id : map.keySet()) {
            if (sb.length() == 0) {
                sb.append("LET entities = [{_key: '");
                sb.append(id.split("/")[1]).append("', c: ").append(map.get(id)).append("}");
            } else {
                sb.append(", {_key: '").append(id.split("/")[1]).append("', c: ").append(map.get(id)).append("}");
            }
        }
        sb.append("]\nFOR v in entities UPDATE v WITH {degree: v.c} IN ")
                .append(collection).append(" RETURN NEW");
        String aql = sb.toString();
        try {
            List<BaseDocument> docs = business.execute(aql);
            String suffix = "\033[0m";
            String prefix = "\033[31;1m";
            for (BaseDocument doc : docs) {
                Long degree = (Long) doc.getAttribute("degree");
                long d = degree == null ? -1 : (long) degree;
                if (d < 100) continue;
                String name = (String) doc.getAttribute("name");
                System.out.println(String.format("document %s-%s has degree %s %d %s",
                        doc.getKey(), name, prefix, d, suffix));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(aql);
        }
    }

}
