package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;
import com.qianzhan.qichamao.util.DbConfigBus;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class RedisCompanyIndexWriter extends BaseWriter {

    private Jedis predis;   // code -> name
    private Jedis nredis;   // name -> code
    private int pDbIndex;
    private int nDbIndex;
    public RedisCompanyIndexWriter() throws Exception {
        super("config/RedisComIndex.txt");
        checkpointName = "data-adaptor.redis.company";
        init();
    }

    private void init() {
        pDbIndex = DbConfigBus.getDbConfig_i("redis.db.positive", 1);
        nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);
//        RedisClient.registerClient(pDbIndex);
        RedisClient.registerClient(nDbIndex);
//        predis = RedisClient.get(pDbIndex);
        nredis = RedisClient.get(nDbIndex);
    }

    private void preCreate() {
        // register hook
        preHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        postHooks = new ArrayList<>();
        if ((tasktype & TaskType.arango.getValue()) != 0) {
            ArangodbCompanyWriter.bulkInsert(tasks_key);
        }
        postHooks.add(() -> SharedData.closeBatch(tasks_key));
    }

    private boolean createInner() {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, 5000);
        if (companies.size() == 0) return false;

        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            char codeTail = company.oc_code.charAt(8);
            String area = company.oc_area;
            if (codeTail == 'T' || codeTail == 'K'
                    || area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }

            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);
            cp.r_com.setCode(company.oc_code);
            cp.r_com.setArea(company.oc_area);
            cp.r_com.setName(company.oc_name);

            // only insert Company vertices from OrgCompanyList table
            ArangoCpPack a_com = cp.a_com;
            if (a_com != null) {
                a_com.com = new ArangoCpVD(company.oc_code, company.oc_name, company.oc_area);
            }

            SharedData.close(tasks_key);
        }

        List<ComPack> cps = SharedData.getBatch(tasks_key);
//        String[] codenames = new String[cps.size()*2];
        String[] namecodes = new String[cps.size()*2];
        for (int i = 0; i < cps.size(); ++i) {
            RedisCompanyIndex r_com = cps.get(i).r_com;
//            codenames[2*i] = codenamepair[0];
//            codenames[2*i+1] = codenamepair[1];
            namecodes[2*i] = r_com.getName();
            namecodes[2*i+1] = r_com.getCode()+r_com.getArea();
        }
//         positive direction: code -> name
//        predis.mset(codenames);
        long r = nredis.msetnx(namecodes);
        if (r == 0) {
            for (int i = 0; i < namecodes.length/2; ++i) {
                if (nredis.exists("s:"+namecodes[2*i])) {
                    nredis.sadd("s:"+namecodes[2*i], namecodes[2*i+1]);
                } else {
                    String old = nredis.get(namecodes[2*i]);
                    if (old != null) {
                        nredis.del(namecodes[2*i]);
                        nredis.sadd("s:"+namecodes[2*i], old, namecodes[2*i+1]);
                    } else {
                        nredis.set(namecodes[2*i], namecodes[2*i+1]);
                    }
                }
            }
        }
        MybatisClient.updateCheckpoint(checkpointName, checkpoint);
        return true;
    }



    //============= provides some intrusive interface =============

    public void setCodenames(List<String> codenames) {
        predis.mset(codenames.toArray(new String[codenames.size()]));
    }

//    /**
//     *
//     * @param code oc_code + oc_area
//     * @param name
//     */
//    public void setCodename(String code, String name) {
//        predis.set(code, name);
//    }

    /**
     *
     * @param name
     * @param code oc_code + oc_area
     */
    public void setNamecode(String name, String code) {
        if (nredis.exists("s:"+name)) {
            nredis.sadd("s:"+name, code);
        } else {
            String old = nredis.get(name);
            if (old != null) {
                nredis.del(name);
                nredis.sadd("s:"+name, old, code);
            } else {
                nredis.set(name, code);
            }
        }
    }

    //============= provides some intrusive interface =============
}
