package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.EsGeoPoint;
import com.qianzhan.qichamao.entity.OrgCompanyGeo;
import org.elasticsearch.common.geo.GeoPoint;

public class ComGeo extends ComBase {
    public ComGeo(String key) {
        super(key);
    }
    @Override
    public void run() {
        if (compack.e_com != null) {
            EsCompany c = compack.e_com;
            OrgCompanyGeo geo = MybatisClient.getCompanyGeo(c.getOc_code());

            if (geo != null) {
                GeoPoint point = new GeoPoint(geo.latitude, geo.longitude);
                c.setCoordinate(point);
            }
        }

        countDown();
    }
}
