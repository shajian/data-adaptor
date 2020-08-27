package com.qcm.task.maintask;

import com.qcm.dal.RedisClient;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.entity.OrgCompanyList;
import com.qcm.entity.OrgCompanyUpdateMeta;
import com.qcm.entity.RedisCompanyIndex;
import com.qcm.es.entity.EsUpdateLogEntity;
import com.qcm.es.repository.EsUpdateLogRepository;
import com.qcm.es.entity.EsComEntity;
import com.qcm.es.search.EsComTripleSearcher;
import com.qcm.es.search.EsUpdateLogSearcher;
import com.qcm.task.specialtask.ComListTask;
import com.qcm.task.specialtask.BaseTask;
import com.qcm.util.Cryptor;
import com.qcm.util.MiscellanyUtil;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

public class RedisComTask extends com.qcm.task.maintask.BaseTask {
    // trace all set-type keys for high efficiency.
    // each of these keys is a company name shared by many oc_codes which are also value of the key
    private Set<String> allSetKeys;
    private String allSetKeysKey = "setkeys";

    public RedisComTask() throws Exception {
        super("config/Task_Redis_CompanyIndex.txt");
        checkpointName = "data-adaptor.redis.company";

        init();
    }

    private void init() {
        allSetKeys = RedisClient.sscan(allSetKeysKey);
    }


    protected void state1_pre() {
        // register hook
        preHooks = new ArrayList<>();
        postHooks = new ArrayList<>();
        preHooks.add(() -> SharedData.openBatch(task));

        postHooks.add(() -> SharedData.closeBatch(task));
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

    /**
     * checkpoint: 146586804
     * @return
     * @throws Exception
     */
    protected boolean state1_inner() throws Exception {
        List<OrgCompanyList> companies = MybatisClient.getCompanies(checkpoint, batch);
        if (companies.size() == 0) return false;

//        int count = 0;
//        BaseTask.resetLatch(task, batch);
        int old = checkpoint;
        for (OrgCompanyList company : companies) {
            if (company.oc_id > checkpoint) checkpoint = company.oc_id;

            if (!validateCode(company.oc_code)) continue;
//            String area = company.oc_area;
//            if (area.startsWith("71") || area.startsWith("81") || area.startsWith("82")) {
//                continue;
//            }
            //洛宁县城区紫竹后评价超市
            company.oc_name = company.oc_name.trim();
            if (MiscellanyUtil.isBlank(company.oc_name)) continue;
//            if (filter_out(company.oc_name)) continue;

            SharedData.open(task);
            ComPack cp = SharedData.get(task);
            cp.redis = new RedisCompanyIndex();
            cp.redis.setCode(company.oc_code);
            cp.redis.setName(Cryptor.md5(company.oc_name));
            cp.redis.setArea(company.oc_area);

//            // for the sake of some unknown error, companies sharing a same name are not all valid,
//            //  (e.g. company status is abnormal), we will try to keep the valid in priority.
//            pool.execute(new ComDtlTask(task));        // to get validity
//            count++;


            SharedData.close(task);
        }



//        int diff = batch-count;
//        while(BaseTask.getLatch(task).getCount() != diff) {
//            Thread.sleep(20);
//        }

        try {
            save2Redis();
        } catch (JedisDataException e) {
            e.printStackTrace();
            Thread.sleep(1000*60);
            checkpoint = old;
        }
        return true;
    }

    private void save2Redis() {
        save2Redis(SharedData.getBatch(task));
    }
    /**
     * we only remove those invalid companies if they share a same company name with some other valid companies
     * it also means, those companies saved in a set-typed key must be all valid.
     * !!!!!! It must be noticed that, if a string-typed key has a invalid company, it do not mean this company
     *      is unique, because there are other same named and invalid companies, but no valid one, and which
     *      invalid one is stored is randomly!!!!!!
     */
    private void save2Redis(List<ComPack> cps) {
        if (cps == null || cps.size() == 0) return;

        Map<String, Set<String>> truesetmap = new HashMap<>();  // these data will save as set-typed pairs in redis
        Map<String, Set<String>> fakesetmap = new HashMap<>();  // only

//        List<ComPack> cps = SharedData.getBatch(task);
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
        // 常德市中汇建材贸易有限公司
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

    protected void state2_pre() throws Exception {
        EsUpdateLogEntity log = EsUpdateLogSearcher.getLastLog(this.task);
        if (log == null) {
            checkpoint = 0;
        } else {
            checkpoint = log.tbl_id;
        }
    }
    protected boolean state2_inner() throws Exception {
        List<OrgCompanyUpdateMeta> metas = MybatisClient.getCompanyUpdateMeta(checkpoint, batch);
        if (metas.size() == 0) return false;

        List<OrgCompanyUpdateMeta> newMetas = new ArrayList<>();
        List<EsUpdateLogEntity> entities = new ArrayList<>();
        List<String> codes = new ArrayList<>();

        for (OrgCompanyUpdateMeta meta : metas) {
            if ("OrgCompanyList".equals(meta.table_name)) {
                EsUpdateLogEntity entity = ComUtil.transfer(task, meta);
                if ("oc_code".equals(meta.field_names)) {
                    codes.add(meta.field_names);
                    entity.status = true;
                    newMetas.add(meta);
                } else {
                    entity.status = false;
                    entity.remark = "field_names is not equal to `oc_code`";
                }
                entities.add(entity);
            }
        }

        if (codes.size() > 0) {
            BaseTask.resetLatch(task, batch);
            int count = 0;
            for (String code : codes) {
                SharedData.open(task);
                ComPack cp = SharedData.get(task);
                cp.redis = new RedisCompanyIndex();
                cp.redis.setCode(code);
                pool.execute(new ComListTask(task));
                SharedData.close(task);
                count++;
            }
            int diff = batch-count;
            while(com.qcm.task.specialtask.BaseTask.getLatch(task).getCount() != diff) {
                Thread.sleep(30);
            }

            List<String> candidateKeys = new ArrayList<>();
            Map<String, ComPack> news = new HashMap<>();
            Map<String, String> removeMap = new HashMap<>();
            List<String> removeList = new ArrayList<>();
            List<ComPack> inserts = new ArrayList<>();
            List<String> esCodes = new ArrayList<>();
            for (ComPack p : SharedData.getBatch(task)) {
                String md5 = Cryptor.md5(p.redis.getName());
                if (allSetKeys.contains(md5)) {
                    candidateKeys.add("s:"+md5);
                    news.put("s:"+md5, p);
                } else {
                    candidateKeys.add(md5);
                    news.put(md5, p);
                }
            }
            if (candidateKeys.size() > 0) {
                List<String> keyValues = new ArrayList<>();
                Map<String, String> updates = new HashMap<>();
                List<String> values = RedisClient.mget(candidateKeys.toArray(new String[1]));
                for (int i = 0; i < candidateKeys.size(); i++) {
                    String value = values.get(i);
                    String key = candidateKeys.get(i);
                    ComPack cp = news.get(key);
                    if (value == null) {
                        esCodes.add(cp.redis.getCode());
                    } else {
                        String newVal = cp.redis.getCode() + cp.redis.getArea();
                        if (!MiscellanyUtil.equals(value, newVal)) {
                            // update value
                            if (key.startsWith("s:")) {
                                removeMap.put(key, value);
                                updates.put(key, newVal);
                            } else {
                                keyValues.add(key);
                                keyValues.add(newVal);
                            }
                        }
                        // if two values equal, nothing needs to do

                        news.remove(key);   // remove those keys had been handled
                    }
                }

                if (esCodes.size() > 0) {
                    Map<String, EsComEntity> olds = new HashMap<>();
                    List<EsComEntity> oldEntities = EsComTripleSearcher.mget(esCodes);
                    if (oldEntities != null && oldEntities.size() > 0) {
                        for (EsComEntity old : oldEntities) {
                            olds.put(old.code, old);
                        }
                    }
                    for (String key : news.keySet()) {
                        ComPack cp = news.get(key);
                        EsComEntity old = olds.get(cp.redis.getCode());
                        if (old != null) {
                            String oldKey = Cryptor.md5(old.name);
                            if (allSetKeys.contains(oldKey)) {
                                removeMap.put("s:"+oldKey, old.name+old.area);
                            } else {
                                removeList.add(oldKey);
                            }
                        }
                        inserts.add(cp);
                    }
                }

                if (removeList.size() > 0) {
                    RedisClient.del(removeList.toArray(new String[1]));
                }
                for (String key : removeMap.keySet()) {
                    RedisClient.srem(key, removeMap.get(key));
                }
                for (String key : updates.keySet()) {
                    RedisClient.sadd(key, updates.get(key));
                }

                if (keyValues.size() > 0) {
                    RedisClient.mset(keyValues.toArray(new String[2]));
                }
                save2Redis(inserts);
            }
        }



        // add updating log into ES
        if (entities.size() > 0)
            EsUpdateLogRepository.singleton().index(entities);
        return true;
    }

    private void diff() {

    }

    //============= provides some intrusive interface =============
}
