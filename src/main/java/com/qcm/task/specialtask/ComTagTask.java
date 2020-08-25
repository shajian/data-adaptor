package com.qcm.task.specialtask;

import com.qcm.collection.TopNCollection;
import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.entity.OrgCompanyTag;
import com.qcm.task.maintask.TaskType;
import com.qcm.util.MiscellanyUtil;
import com.qcm.dal.mybatis.MybatisClient;

import java.util.ArrayList;
import java.util.List;

public class ComTagTask extends BaseTask {
    public ComTagTask(TaskType key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.es != null) {
            EsCompanyEntity c = compack.es;
            List<OrgCompanyTag> tags = MybatisClient.getCompanyTags(c.getOc_code());
            List<String> unweighted_tags = new ArrayList<>();
            TopNCollection<OrgCompanyTag> coll = new TopNCollection(3, OrgCompanyTag.comparator);
            for (OrgCompanyTag tag : tags) {
                if (MiscellanyUtil.isBlank(tag.brandname)) continue;
                if (!tag.isvalid) continue;
                unweighted_tags.add(tag.brandname);
                coll.put(tag);
            }
            c.setTags(unweighted_tags);
            List<OrgCompanyTag> weighted_tags = coll.getUnsafe();
            if (weighted_tags.size()>0) {
                c.setTag_1(weighted_tags.get(0).brandname);
                c.setScore_1(weighted_tags.get(0).score);
                if (weighted_tags.size()>1) {
                    c.setTag_2(weighted_tags.get(1).brandname);
                    c.setScore_2(weighted_tags.get(1).score);
                    if (weighted_tags.size()>2) {
                        c.setTag_3(weighted_tags.get(2).brandname);
                        c.setScore_3(weighted_tags.get(2).score);
                    }
                }
            }
        }

        countDown();
    }
}
