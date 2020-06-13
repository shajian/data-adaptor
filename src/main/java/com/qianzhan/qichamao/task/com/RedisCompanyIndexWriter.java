package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;
import com.qianzhan.qichamao.graph.ArangoBusinessCompany;
import com.qianzhan.qichamao.graph.ArangoBusinessPack;
import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.arangodb.ArangoComClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.*;

public class RedisCompanyIndexWriter extends BaseWriter {
    // trace all set-type keys for high efficiency.
    // each of these keys is a company name shared by many oc_codes which are also value of the key
    private Set<String> allSetKeys;
    private String allSetKeysKey = "setkeys";

    public RedisCompanyIndexWriter() throws Exception {
        super("config/RedisCompanyIndex.txt");
        checkpointName = "data-adaptor.redis.company";

        init();
    }

    private void init() {
//        nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);


        allSetKeys = RedisClient.sscan(allSetKeysKey);
    }


    protected void state1_pre() {
        // register hook
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        for (int task : tasks) {
            if ((task & TaskType.arango.getValue()) != 0) {
                preHooks.add(()-> ArangoComClient.getSingleton().initGraph());
                postHooks.add(()->ArangodbCompanyWriter.insert_static(tasks_key));
            }
        }
        postHooks.add(() -> SharedData.closeBatch(tasks_key));
    }

    // =========================

    /**
     * There are two kinds of types for key. The first is single string and the second is string set.
     * If a group of companies shares a same name, they are stored in string-set typed key.
     * `allSetKeys` keeps all string-set typed keys currently.
     * Storage is done by the following steps:
     * 1. if a company name is in `allSetKeys`, then add it in `truesetmap`; otherwise, add it in `fakesetmap`.
     * 2. Iterate all companies and do as step one, after that, iterate `fakesetmap` and if a value is a set with
     *      size > 1, then the key should be string-set typed key, so restore it in `truesetmap` and add the key in
     *      `allSetKeys` to trace all string-set typed keys in time, and if the value set is with size == 1, then add
     *      the key and value in an array for RedisClient.msetnx them in the future.
     * 3. RedisClient.msetnx the array of key and value. If failed, it means there are some repeated keys that
     *      conflicted with redis database, so RedisClient.getSet the key and value one pair by one pair, and if
     *      confliction happens, delete the key and add it in `truesetmap`. Remember, add it in `allSetKeys`, too.
     * 4. Handling `truesetmap`, e.g. RedisClient.sadd each key-value pair.
     */

    // =========================


    protected boolean state1_inner() throws Exception {
        if (checkpoint >= 109098118) return false;

        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

//        int count = 0;
//        ComBase.resetLatch(tasks_key, batch);
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
//            String area = company.oc_area;
//            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
//                continue;
//            }
            company.oc_name = company.oc_name.trim();
            if (MiscellanyUtil.isBlank(company.oc_name)) continue;
//            if (filter_out(company.oc_name)) continue;

            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);
            cp.redis = new RedisCompanyIndex();
            cp.redis.setCode(company.oc_code);
            cp.redis.setName(company.oc_name);
            cp.redis.setArea(company.oc_area);

//            // for the sake of some unknown error, companies sharing a same name are not all valid,
//            //  (e.g. company status is abnormal), we will try to keep the valid in priority.
//            pool.execute(new ComDtl(tasks_key));        // to get validity
//            count++;

            // check if arango task is turned on
            ArangoBusinessPack arango = cp.arango;
            if (arango != null) {
                if (GlobalConfig.getEnv() == 1) {
                    ArangoCpPack a_com = arango.legacyPack;
                    if (a_com != null) {
                        a_com.com = new ArangoCpVD(company.oc_code, company.oc_name, company.oc_area);
                    }
                } else {
                    arango.company = new ArangoBusinessCompany(company.oc_code, company.oc_name, company.oc_area);
                }
            }


            SharedData.close(tasks_key);
        }



//        int diff = batch-count;
//        while(ComBase.getLatch(tasks_key).getCount() != diff) {
//            Thread.sleep(20);
//        }

        save2Redis();

        return true;
    }

    /**
     * we only remove those invalid companies if they share a same company name with some other valid companies
     * it also means, those companies saved in a set-typed key must be all valid.
     * !!!!!! It must be noticed that, if a string-typed key has a invalid company, it do not mean this company
     *      is unique, because there are other same named and invalid companies, but no valid one, and which
     *      invalid one is stored is randomly!!!!!!
     */
    public void save2Redis() {
        Map<String, Set<String>> truesetmap = new HashMap<>();  // these data will save as set-typed pairs in redis
        Map<String, Set<String>> fakesetmap = new HashMap<>();  // only

        List<ComPack> cps = SharedData.getBatch(tasks_key);
        for (ComPack cp : cps) {
            RedisCompanyIndex com = cp.redis;

            // for efficiency, we do not handle via ComPack here, but
            // use a map to categorize company names.
            String key = com.getName();

            String value = com.getCode()+com.getArea();

            if (allSetKeys.contains(key)) {     // this key may be set-type
//                if (!com.isValid()) continue;   // sorry, you are passed, since you are invalid

                if (truesetmap.containsKey(key)) {  // add into map directly
                    truesetmap.get(key).add(value);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(value);
                    truesetmap.put(key, set);
                }
            } else {
                if (fakesetmap.containsKey(key)) {  // already has a same named company
//                    if (!com.isValid()) continue;   // sorry, you are passed

                    // check if the early added key-value is valid
                    Set<String> set = fakesetmap.get(key);
                    set.add(value);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(value);
                    fakesetmap.put(key, set);
                }
            }
        }

        // after archiving, prepare to save to redis
        List<String> namecodes = new ArrayList<>();
        Set<String> newAdd = new HashSet<>();           // newly added into truesetmap
        for (String key : fakesetmap.keySet()) {
            Set<String> set = fakesetmap.get(key);
            if (set.size() == 1) {                  // add as a single string-type
                namecodes.add(key);
                namecodes.add(set.iterator().next());
            } else {                                // move it into set-type
                truesetmap.put(key, set);
                newAdd.add(key);                    // record new added set-typed key in this batch
            }
        }

        if (namecodes.size() > 0) {
            String[] values = namecodes.toArray(new String[0]);
            long r = RedisClient.msetnx(values);    // bulk set/multi-set if all are not existed
            if (r == 0) {   // batch set failed
                for (int i = 0; i < values.length/2; ++i) {
                    String key = values[2*i];
                    String value = values[2*i+1];
                    String old = RedisClient.getSet(key, value);// substitute old with new and return old
                    // if old is null or equal to new value, then nothing else should do;
                    //  or else, remove new value from redis, and reset them as set-typed pair.
                    if (old != null) {    // already exists and is valid
                        String oldCode = old.substring(0, 9);
                        String newCode = value.substring(0, 9);
                        if (!oldCode.equals(newCode)) {     // both old and new are not equal(area is not concerned)
                            // move them into set-type key
                            RedisClient.del(key);
                            newAdd.add(key);
                            Set<String> oldnew = new HashSet<>();
                            oldnew.add(old);
                            oldnew.add(value);
                            truesetmap.put(key, oldnew);
                        }
                    }
                }
            }
        }

        if (truesetmap.size() > 0) {
            for (String key : truesetmap.keySet()) {
                RedisClient.sadd("s:"+key, truesetmap.get(key).toArray(new String[0]));
            }
        }
        if (newAdd.size() > 0) {
            RedisClient.sadd(allSetKeysKey, newAdd.toArray(new String[0]));
            allSetKeys.addAll(newAdd);
        }
    }


    /**
     *
     * @param name
     * @param code oc_code + oc_area
     */
    public void setNamecode(String name, String code) {
        if (RedisClient.exists("s:"+name)) {
            RedisClient.sadd("s:"+name, code);
        } else {
            String old = RedisClient.get(name);
            if (old != null) {
                RedisClient.del(name);
                RedisClient.sadd("s:"+name, old, code);
            } else {
                RedisClient.set(name, code);
            }
        }
    }

    //============= provides some intrusive interface =============
}
