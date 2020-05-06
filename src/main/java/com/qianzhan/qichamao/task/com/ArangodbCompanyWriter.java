package com.qianzhan.qichamao.task.com;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.qianzhan.qichamao.dal.arangodb.ArangoComClient;
import com.qianzhan.qichamao.dal.arangodb.ArangoComInput;
import com.qianzhan.qichamao.dal.mongodb.MongoClientRegistry;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.util.BeanUtil;
import com.qianzhan.qichamao.util.Cryptor;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.util.NLP;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class ArangodbCompanyWriter extends BaseWriter {
    private static ArangoComClient client = ArangoComClient.getSingleton();

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


    public void upsert() {
        upsert_static(this.tasks_key, this.state);
    }

    public static void upsert_static(String tasks_key, int state) {
        List<ComPack> cps = SharedData.getBatch(tasks_key);
        // vertex which has code as its key
        Map<String, ArangoCpVD> cannot_bulk_insert = new HashMap<>(cps.size());
        // vertex which has no code, so its key is composed by `related company's code`+`md5(name)`
        // this kind of vertices can be bulk inserted
        Map<String, ArangoCpVD> can_bulk_insert = new HashMap<>(cps.size());
        Map<String, ArangoCpED> edges = new HashMap<>(cps.size());
        for (ComPack cp : cps) {
            ArangoCpPack p = cp.a_com;
            String code = p.oc_code;
            if (MiscellanyUtil.isBlank(code)) {
                if (p.com != null) code = p.com.getKey();
                if (MiscellanyUtil.isBlank(code)) {
                    throw new ValueException("ArangoCpPack.oc_code can not be null or empty if you want to " +
                            "execute Arangodb writing task. Please set ArangoCpPack.oc_code or " +
                            "ArangoCpPack.com");
                }
            }
            // add main company vertex
            if (p.com != null) {
                if (state == 1) {
                    if (!can_bulk_insert.containsKey(code)) {
                        can_bulk_insert.put(code, p.com);
                    }
                } else {
                    if (!cannot_bulk_insert.containsKey(code)) {
                        cannot_bulk_insert.put(code, p.com);
                    }
                }
            }


            // add legal person vertex
            if (p.lps != null) {
                for (int i = 0; i < p.lps.size(); i++) {
                    ArangoCpVD v = p.lps.get(i);        // legal person vertex
                    ArangoCpED e = p.lp_edges.get(i);
                    // `code` is oc_code of main company
                    // legal person of main company starting with `code` means that
                    //  it is not independent.
                    if (v.getKey().startsWith(code)) {     // dependent
                        if (!can_bulk_insert.containsKey(v.getKey())) {
                            can_bulk_insert.put(v.getKey(), v);
                        }
                    } else {    // independent, because this lp vertex must has key of its own oc_code
                        if (!cannot_bulk_insert.containsKey(v.getKey())) {
                            cannot_bulk_insert.put(v.getKey(), v);
                        }
                    }
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    }
                }
            }

            // add share holder vertex
            if (p.share_holders != null) {
                for (int i = 0; i < p.share_holders.size(); i++) {
                    ArangoCpVD v = p.share_holders.get(i);
                    ArangoCpED e = p.sh_edges.get(i);
                    if (v.getKey().startsWith(code)) {     // dependent
                        if (!can_bulk_insert.containsKey(v.getKey())) {
                            can_bulk_insert.put(v.getKey(), v);
                        }
                    } else {                    // independent
                        if (!cannot_bulk_insert.containsKey(v.getKey())) {
                            cannot_bulk_insert.put(v.getKey(), v);
                        }
                    }
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    } else {    // combine money and ratio of the same share holder
                        ArangoCpED old = edges.get(e.getKey());
                        old.setMoney(old.getMoney()+e.getMoney());
                        old.setRatio(old.getRatio()+e.getRatio());
                    }
                }
            }

            if (p.senior_members != null) {
                for (int i = 0; i < p.senior_members.size(); i++) {
                    ArangoCpVD v = p.senior_members.get(i);
                    ArangoCpED e = p.sm_edges.get(i);
                    if (v.getKey().startsWith(code)) {     // dependent
                        if (!can_bulk_insert.containsKey(v.getKey())) {
                            can_bulk_insert.put(v.getKey(), v);
                        }
                    } else {                    // independent
                        if (!cannot_bulk_insert.containsKey(v.getKey())) {
                            cannot_bulk_insert.put(v.getKey(), v);
                        }
                    }
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    } else {        // combine positions of the same member
                        ArangoCpED old = edges.get(e.getKey());
                        old.setPosition(old.getPosition()+","+e.getPosition());
                    }
                }
            }

            if (p.contact_edges != null) {
                for (ArangoCpED e : p.contact_edges) {
                    if (!edges.containsKey(e.getKey())) {
                        edges.put(e.getKey(), e);
                    }
                }
            }
        }

        // for dependent vertices and edges, because their key are different from each other, so bulk insert them.
        List<BaseDocument> docs = new ArrayList<>(can_bulk_insert.size());
        List<BaseEdgeDocument> edocs = new ArrayList<>(edges.size());
        for (String key : can_bulk_insert.keySet()) {
            docs.add(can_bulk_insert.get(key).to());
        }
        for (String key : edges.keySet()) {
            edocs.add(edges.get(key).to());
        }
        try {
            client.bulkInsert_VD(docs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            client.bulkInsert_ED(edocs);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // for independent vertices, it must use `upsert` operation
        try {
            client.upsert_VD(cannot_bulk_insert.values());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    protected boolean state1_inner() {
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

            ArangoCpPack a_com = cp.a_com;
            a_com.oc_code = company.oc_code;
            a_com.com = new ArangoCpVD(company.oc_code, company.oc_name, company.oc_area);
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

        ComBase.resetLatch(tasks_key, batch*4);
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

            ArangoCpPack a_com = cp.a_com;
            a_com.oc_code = company.oc_code;
            a_com.oc_area = company.oc_area;
//            a_com.com = new ArangoCpVD(company.oc_code, company.oc_name, company.oc_area);

            //
            pool.execute(new ComDtl(tasks_key));
            pool.execute(new ComMember(tasks_key));
            pool.execute(new ComShareHolder(tasks_key));
            pool.execute(new ComContact(tasks_key));

            SharedData.close(tasks_key);
            count++;
        }
        // wait for all sub-tasks finishing
        int diff = (batch-count)*4;
        while(ComBase.getLatch(tasks_key).getCount() != diff) {
            Thread.sleep(100);
        }

        System.out.println("writing lp, sh, sm into arango...");

        upsert();

        return true;
    }

    protected void state4_pre() throws Exception {
        path_thre = config.getInt("path_thre", 3);
        max_traverse_depth = config.getInt("max_traverse_depth", path_thre);
//        readSubtable();
    }


    /**
     * combine vertices with a same name
     * @return
     * @throws Exception
     */
    protected boolean state4_inner() throws Exception {
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

            combine(company.oc_code);
        }
        return true;
    }

    /**
     * combine two vertices with a same name
     * @param code oc_code of a vertex in a chain
     */
    private void combine(String code) {
        ArangoComInput input = new ArangoComInput(client.getVertexCollName()+"/"+code,
                max_traverse_depth);
        // 1. do not concern with any `share` vertex; 2. get natural person or unknown company
        input.setFilter("FILTER !e.share && CHAR_LENGTH(v._key) > 9");
        List<String> ids = client.chain_VD(input);      // get all vertices of this chain
        if (ids.size() == 0) return;

        //

        // group vertex ids by name(actually name_md5)
        Map<String, List<String>> groups = new HashMap<>(); // keep all natural persons and unknown companies
        for (String id : ids) {
            String key = id.split("/")[1];
            if (key.length() <= 9) continue;
            String name_md5 = key.substring(9);
            List<String> group = groups.get(name_md5);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(name_md5, group);
            }
            group.add(id);
        }

        // get the manual rules for current oc_code
        List<MongoPersonAgg> ps = MongoClientRegistry.client("agg").findBy("code1", "code2", code);
        Map<String, String> white_list = new HashMap<>();
        Map<String, String> black_list = new HashMap<>();
        for (MongoPersonAgg p : ps) {
            String other_code = p.code1.equals(code) ? p.code2 : p.code1;
            String name_md5 = Cryptor.md5(p.name);
            if (p.status == 1) {
                white_list.put(other_code, name_md5);
            } else {
                black_list.put(other_code, name_md5);
            }
        }

        for (String name_md5 : groups.keySet()) {
            List<String> group = groups.get(name_md5);
            if (group.size() == 1) continue;    //

            for (int i = 0; i < group.size()-1; i++) {  // in a group, each two vertices may be combined
                String start_id = group.get(i);
                if (start_id == null) continue;
                String start_code = start_id.substring(3, 12);
                for (int j = i+1; j < group.size(); j++) {
                    String end_id = group.get(j);
                    if (end_id == null) continue;

                    boolean combine = false;
                    if (start_code.equals(code)) {
                        String end_code = end_id.substring(3, 12);
                        if (name_md5.equals(black_list.get(end_code))) {
                            continue;
                        }
                        if (name_md5.equals(white_list.get(end_code))) {
                            combine = true;
                        }
                    }

                    if (!combine) {
                        ArangoComInput sp = new ArangoComInput(start_id, end_id);
                        List<String> path = client.shortestPath(sp);
                        if (path.size() <= path_thre+1) {   // combine j to i
                            combine = true;
                        }
                    }
                    if (combine) {
                        // olds: old edges(from and to)
                        List<List<String>> olds = client.updateFrom(start_id, end_id);
                        client.delete_VD(end_id.split("/")[1]);
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
