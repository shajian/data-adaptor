package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.es.EsCompanyEntity;
import com.qianzhan.qichamao.entity.OrgCompanyGeo;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import org.elasticsearch.common.geo.GeoPoint;

public class ComGeo extends ComBase {
    public ComGeo(String key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.es != null) {
            EsCompanyEntity c = compack.es;
            OrgCompanyGeo geo = MybatisClient.getCompanyGeo(c.getOc_code());

            if (geo != null) {
                GeoPoint point = new GeoPoint(geo.latitude, geo.longitude);
                c.setCoordinate(point);
            }
        }

        countDown();
    }
}
