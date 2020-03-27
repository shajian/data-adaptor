package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.collection.TopNCollection;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyTag;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class EsComTag extends EsComBase {
    @Override
    public Boolean call() throws Exception {
//        if (EsConfigBus.getTaskConfigBool("local")) return true;
        if (getCompany() != null) {
            EsCompany c = getCompany();
            List<OrgCompanyTag> tags = MybatisClient.getCompanyTags(c.getOc_code());
            List<String> unweighted_tags = new ArrayList<>();
            TopNCollection<OrgCompanyTag> coll = new TopNCollection(3, OrgCompanyTag.comparator);
            for (OrgCompanyTag tag : tags) {
                if (MiscellanyUtil.isBlank(tag.brandname)) continue;
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
        return true;
    }
}
