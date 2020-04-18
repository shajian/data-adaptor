package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.dal.arangodb.ArangoComClient;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.ArangoCpPack;
import com.qianzhan.qichamao.entity.ArangoCpVD;
import com.qianzhan.qichamao.entity.OrgCompanyList;
import com.qianzhan.qichamao.entity.RedisCompanyIndex;
import com.qianzhan.qichamao.util.DbConfigBus;

import java.util.*;
import java.util.regex.Pattern;

public class RedisCompanyIndexWriter extends BaseWriter {
    private int pDbIndex;
    private int nDbIndex;
    // trace all set-type keys for high efficiency.
    private Set<String> allSetKeys;
    private String allSetKeysKey = "setkeys";

    public RedisCompanyIndexWriter() throws Exception {
        super("config/RedisCompanyIndex.txt");
        checkpointName = "data-adaptor.redis.company";

        init();
    }

    private void init() {
        nDbIndex = DbConfigBus.getDbConfig_i("redis.db.negative", 2);


        allSetKeys = RedisClient.sscan(allSetKeysKey);
    }


    protected void preCreate() {
        // register hook
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(tasks_key));

        for (int task : tasks) {
            if ((task & TaskType.arango.getValue()) != 0) {
                preHooks.add(()-> ArangoComClient.getSingleton().initGraph());
                postHooks.add(()->ArangodbCompanyWriter.bulkInsert(tasks_key));
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


    protected boolean createInner() {
        if (checkpoint >= 109098118) return false;

        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

        Map<String, Set<String>> truesetmap = new HashMap<>();
        Map<String, Set<String>> fakesetmap = new HashMap<>();
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;
            if (company.oc_code.length() != 9) {
                System.out.println("=========="+company.oc_code);
                continue;
            }
            char codeTail = company.oc_code.charAt(8);
            String area = company.oc_area;
            if (codeTail == 'T' || codeTail == 'K'
                    || area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
                continue;
            }

            if (filter_out(company.oc_name)) continue;

            SharedData.open(tasks_key);
            ComPack cp = SharedData.get(tasks_key);
//            cp.r_com.setCode(company.oc_code);
//            cp.r_com.setArea(company.oc_area);
//            cp.r_com.setName(company.oc_name);
            // for efficiency, we do not handle via ComPack here, but
            // use a map to categorize company names.
            String key = company.oc_name;
            // In database, oc_code is used as unique key, so it is impossible that
            // two different oc_codes share a same oc_name. This property is very import
            // because it avoids that the difference is made only by oc_area.
            String value = company.oc_code + company.oc_area;
            if (allSetKeys.contains(key)) {
                if (truesetmap.containsKey(key)) {
                    truesetmap.get(key).add(value);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(value);
                    truesetmap.put(key, set);
                }
            } else {
                if (fakesetmap.containsKey(key)) {
                    fakesetmap.get(key).add(value);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(value);
                    fakesetmap.put(key, set);
                }
            }


            // only insert Company vertices from OrgCompanyList table
            ArangoCpPack a_com = cp.a_com;
            if (a_com != null) {
                a_com.com = new ArangoCpVD(company.oc_code, company.oc_name, company.oc_area);
            }

            SharedData.close(tasks_key);
        }

        List<String> namecodes = new ArrayList<>();
        Set<String> newAdd = new HashSet<>();
        for (String key : fakesetmap.keySet()) {
            Set<String> set = fakesetmap.get(key);
            if (set.size() == 1) {
                namecodes.add(key);
                namecodes.add(set.iterator().next());
            } else {
                truesetmap.put(key, set);
                newAdd.add(key);
            }
        }

        if (namecodes.size() > 0) {
            String[] values = namecodes.toArray(new String[0]);
            long r = RedisClient.msetnx(values);
            if (r == 0) {   // batch set failed
                for (int i = 0; i < values.length/2; ++i) {
                    String key = values[2*i];
                    String value = values[2*i+1];
                    String old = RedisClient.getSet(key, value);
                    if (old != null && !old.equals(value)) {
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

        if (truesetmap.size() > 0) {
            for (String key : truesetmap.keySet()) {
                RedisClient.sadd("s:"+key, truesetmap.get(key).toArray(new String[0]));
            }
        }
        if (newAdd.size() > 0) {
            RedisClient.sadd(allSetKeysKey, newAdd.toArray(new String[0]));
            allSetKeys.addAll(newAdd);
        }
        return true;
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
