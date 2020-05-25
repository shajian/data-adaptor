package com.qianzhan.qichamao.task.com;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.dal.arangodb.ArangoBusinessRepository;
import com.qianzhan.qichamao.dal.arangodb.ArangoInterveneRepository;
import com.qianzhan.qichamao.graph.ArangoBusinessCompany;
import com.qianzhan.qichamao.graph.ArangoBusinessPack;
import com.qianzhan.qichamao.graph.ArangoWriter;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
import com.qianzhan.qichamao.util.parallel.Master;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.OrgCompanyDimBatch;
import com.qianzhan.qichamao.entity.OrgCompanyList;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Function;

public class ArangodbCompanyWriter extends BaseWriter {
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


    public void upsert() throws Exception {
        upsert_static(this.tasks_key);
    }

    public static void upsert_static(String tasks_key) throws Exception {
        ArangoWriter.upsert(SharedData.getBatch(tasks_key));
    }


    public ArangodbCompanyWriter() throws Exception {
        super("config/ArangodbCompany.txt");
        checkpointName = "data-adaptor.arango.company";
        dist_step = config.getInt("dist_step", 1000);
    }

    protected void state1_pre() {
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        postHooks.add(() -> SharedData.closeBatch(tasks_key));

    }

    /**
     * bulk write company vertices into Arangodb
     * @return
     * @throws Exception
     */
    protected boolean state1_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
            String area = company.oc_area;
            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }
            company.oc_name = company.oc_name.trim();
            if (filter_out(company.oc_name)) continue;

            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);

            ArangoBusinessPack a_com = cp.arango;
            a_com.oc_code = company.oc_code;
            a_com.company = new ArangoBusinessCompany(company.oc_code, company.oc_name, company.oc_area);
            SharedData.close(tasks_key);
        }
        System.out.println("writing company vertices into arango...");
        upsert();
        return true;
    }



    protected void state3_pre() {
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        postHooks.add(() -> SharedData.closeBatch(tasks_key));
    }

    /**
     * this is the normal method/process: read OrgCompanyList,
     *  and then multi-threadly read legal person, share holder and senior member
     *  but, only insert/upsert legal person, share holder and senior member into Arangodb.
     * @return
     * @throws Exception
     */
    protected boolean state3_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

        ComBase.resetLatch(tasks_key, batch*3);
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

            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);

            ArangoBusinessPack a_com = cp.arango;
            a_com.oc_code = company.oc_code;
            a_com.oc_area = company.oc_area;
//            arango.com = new ArangoBusinessCompany(company.oc_code, company.oc_name, company.oc_area);

            //
            pool.execute(new ComDtl(tasks_key));
            pool.execute(new ComMember(tasks_key));
            pool.execute(new ComShareHolder(tasks_key));
//            pool.execute(new ComContact(tasks_key));

            SharedData.close(tasks_key);
            count++;
        }
        // wait for all sub-tasks finishing
        int diff = (batch-count)*3;
        while(ComBase.getLatch(tasks_key).getCount() != diff) {
            Thread.sleep(100);
        }

        System.out.println("writing lp, sh, sm into arango..." + new Date().toString());

        upsert();
        Thread.sleep(200);
        return true;
    }

    protected void state4_pre() throws Exception {
        path_thre = config.getInt("path_thre", 3);
        max_traverse_depth = config.getInt("max_traverse_depth", path_thre);
    }


    /**
     * combine vertices with a same name
     * @return
     * @throws Exception
     */
    protected boolean state4_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

//        List<String> toRemoved = new ArrayList<>();
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


            combine(company.oc_code);
        }

//        toRemoved.addAll(getEdgeByContact_Parallel(codes));     // collect all edges
//
//        if (toRemoved.size() > 0) {
//            System.out.println(String.format("%d edges by contact will be removed.", toRemoved.size()));
//            client.bulkDelete_ED(toRemoved);
//        }
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

    /**
     * combine two vertices with a same name
     * @param code oc_code of a vertex in a chain
     */
    private void combine(String code) throws Exception {
        ArangoInterveneRepository intervene = ArangoInterveneRepository.singleton();
        ArangoBusinessRepository business = ArangoBusinessRepository.singleton();
        // because there may be many vertices connect to the code vertex, so we set max_deep=2 here
        List<BaseDocument> vertices = business.connectedGraph(business.getGraphMeta().froms()[0]+"/"+code, 2);
        if (MiscellanyUtil.isArrayEmpty(vertices)) return;

        //

        // group vertex ids by personal name(actually name_md5)
        Map<String, List<BaseDocument>> groups = new HashMap<>(); // keep all natural persons and unknown companies
        for (BaseDocument vertex : vertices) {
            if (vertex == null) continue;
            String key = vertex.getKey();   //.split("/")[1];
            if (key.length() <= 9) continue;    // company-type node, skip

            String name_md5 = key.substring(9);
            List<BaseDocument> group = groups.get(name_md5);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(name_md5, group);
            }
            group.add(vertex);
        }

        for (String name_md5 : groups.keySet()) {
            List<BaseDocument> group = groups.get(name_md5);
            if (group.size() == 1) continue;    // no more than 2 person shares the same name, skip

            for (int i = 0; i < group.size()-1; i++) {  // in a group, each two vertices may be combined
                BaseDocument start_vertex = group.get(i);         // start from a person vertex
                if (start_vertex == null) continue;
                // recall that peronal vertex's key is composed with code of related company and md5 of person name.
                String start_code = start_vertex.getKey().substring(0, 9);  // extract the related company code
                List<BaseEdgeDocument> start_edges = intervene.neighbours(
                        intervene.getGraphMeta().froms()[0] + "/" + start_code);

                for (int j = i+1; j < group.size(); j++) {
                    BaseDocument end_vertex = group.get(j);
                    if (end_vertex == null) continue;

                    String end_code = end_vertex.getKey().substring(0, 9);
                    // get neighbours of `start_code` and `end_code` respectively,
                    //  and if the name in intersection of the two groups of neighbours, then combine

                    boolean combine = false;

                    String e_id = intervene.getGraphMeta().froms()[0]+"/"+end_code;
                    List<BaseEdgeDocument> end_edges = intervene.neighbours(
                        e_id
                    );

                    Map<String, Integer> counts = new HashMap<>();
                    if (end_edges != null) {
                        for (BaseEdgeDocument doc : end_edges) {
                            // edge: 'from' must be a person vertex while 'to' must be a code vertex
                            String from = doc.getFrom();        // so do not need to consider 'to' vertex
                            Integer c = counts.get(from);
                            if (c == null) {
                                counts.put(from, 1);
                            } else {
                                counts.put(from, c + 1);
                            }
                        }
                    }
                    if (start_edges != null) {
                        for (BaseEdgeDocument doc : start_edges) {
                            String from = doc.getFrom();
                            Integer c = counts.get(from);
                            if (c == null) {
                                counts.put(from, 1);
                            } else {
                                counts.put(from, c + 1);
                            }
                        }
                    }
                    int count = 0;
                    for (String key : counts.keySet()) {
                        if (key.endsWith(name_md5)) {
                            count++;
                            if (counts.get(key) == 2) {
                                combine = true;
                                break;
                            }
                        }
                    }
                    if (count == 2) {   // discombine_by_intervene
                        if (combine) {
                            throw new Exception(
                                    String.format("confliction between combine and discombine. " +
                                            "(name_md5, code1, code2) is (%s, %s, %s)",
                                            name_md5, start_code, end_code));
                        }
                        continue;       // discombine, skip and continue
                    }

                    // because we limit the search max_depth=2, so it doesn't need to check the distance
                    //  between the two candidates to be combined. just combine them directly.
                    combine = true;
                    if (combine) {
                        // olds: old edges(from and to)
                        System.out.println(String.format(
                                "prepare to combine %s-%s into %s-%s",
                                end_code, end_vertex.getProperties().get("name"),
                                start_code, start_vertex.getProperties().get("name")));
//                        List<List<String>> olds = client.updateFrom(start_vertex.getId(), end_vertex.getId());
//                        // end_id: check if it is isolated vertex, if is, delete it
//                        //  instead of deleting it directly.
//                        List<ArangoBusinessRelation> eds = client.neighbours(end_vertex.getId());
//                        if (MiscellanyUtil.isArrayEmpty(eds)) {
//                            client.delete_VD(end_vertex.getKey());
//                        }

                        business.merge(end_vertex.getId(), start_vertex.getId(), false);
                        group.set(j, null);
                    }
                }
            }
        }
    }



    @Deprecated
    protected boolean state4_inner_deprecated() throws Exception {
        List<OrgCompanyDimBatch> docs = getLpShSm();
        if (docs == null) return false;     // all sub tables have been read over
        while (docs.size() == 0) {      // current sub table has been read over
            subTableIndex++;
            checkpoint = 0;
            // store the record point
            updateSubtable();
            MybatisClient.updateCheckpoint(checkpointName, checkpoint);
            docs = getLpShSm();
            if (docs == null) return false;
        }
        // read a batch successfully
        for (OrgCompanyDimBatch doc : docs) {
            checkpoint = doc.a_id;
            if (doc.a_status == 4) continue;
            String name = doc.a_name.trim();
            if (MiscellanyUtil.isBlank(name)) continue;

            int flag = NLP.recognizeName(name);
            // not completed...
        }

        return true;
    }
    @Deprecated
    private List<OrgCompanyDimBatch> getLpShSm() {
        List<OrgCompanyDimBatch> docs = null;
        if (subTableIndex == 0) {
            docs = MybatisClient.getCompanyDtls(checkpoint, batch);
            lp_sh_sm = 0;
        } else if (subTableIndex == 1) {        // member
            docs = MybatisClient.getCompanyMemberBatch(checkpoint, batch);
            lp_sh_sm = 2;
        } else if (subTableIndex == 2) {        // share holder
            docs = MybatisClient.getCompanyGDBatch(checkpoint, batch);
            lp_sh_sm = 1;
        } else if (subTableIndex < subTableNames.size()) {
            String table = subTableNames.get(subTableIndex);
            String[] segs = table.split("_");
            if (segs.length == 2) {
                String tail = segs[segs.length - 1];
                if (segs[0].endsWith("GD")) {
                    docs = MybatisClient.getCompanyGDGsxtBatch(checkpoint, batch, tail);
                    lp_sh_sm = 1;
                } else if (segs[0].endsWith("Mgr")) {
                    docs = MybatisClient.getCompanyMemberGsxtBatch(checkpoint, batch, tail);
                    lp_sh_sm = 2;
                }
            }
        }
        return docs;
    }
    @Deprecated
    private void updateSubtable() throws Exception {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("tmp/Arangodb_state4.txt"),
                Charset.forName("utf-8"))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("sub_table_index")) {
                    lines.add(String.format("sub_table_index=%d\n", subTableIndex));
                } else {
                    lines.add(line);
                }
            }
        }
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("tmp/arango_state4.txt"),
                Charset.forName("utf-8"), StandardOpenOption.WRITE)) {
            for (String line : lines) {
                writer.write(line);
            }
        }
    }
    @Deprecated
    private void readSubtable() throws Exception {
        subTableNames = new ArrayList<>();
        File f = new File("tmp/arango_state4.txt");
        if (!f.exists()) {
            System.out.println("file storing sub table names do not exists. It will load from database...");
            subTableIndex = 0;
            // read table
            List<String> tables = MybatisClient.getGsxtSubtableNames();

            subTableNames.add("OrgCompanyDtl");         // from which we read legal person
            subTableNames.add("OrgCompanyDtlMgr");      // from which we read shenzhen's senior member
            subTableNames.add("OrgCompanyDtlGD");       // from which we read shenzhen's share holder
            for (String table : tables) {
                if (table.startsWith("OrgCompanyGsxtDtlGD_")
                        || table.startsWith("OrgCompanyGsxtDtlMgr_")) {
                    subTableNames.add(table);
                }
            }
            // persist on disk
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("tmp/arango_state4.txt"),
                    Charset.forName("utf-8"))) {
                writer.write(String.format("sub_table_index=%d\n", subTableIndex));
                writer.write("# sub table names must be located at last.\n");
                writer.write("sub_table_names:\n");
                for (String name : subTableNames) {
                    writer.write(name+"\n");
                }
            } // throw out exception
        } else {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get("tmp/Arangodb_state4.txt"),
                    Charset.forName("utf-8"))) {
                String line = null;
                boolean subTableStarts = false;
                while((line = reader.readLine()) != null) {
                    if (MiscellanyUtil.isBlank(line) || line.startsWith("#")) continue;

                    if (line.startsWith("sub_table_names:")) {
                        subTableStarts = true;
                    } else if (subTableStarts) {
                        subTableNames.add(line.trim());
                    } else {
                        String[] segs = line.trim().split("=", 2);
                        if (segs[0].equals("sub_table_index")) {
                            subTableIndex = Integer.parseInt(segs[1]);
                        }
                    }
                }
            } // throw out exception
        }
    }

}
