package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.EsGeoPoint;
import com.qianzhan.qichamao.entity.OrgCompanyGeo;

public class EsComGeo extends EsComBase {
    @Override
    public Boolean call() throws Exception {
        if (getCompany() != null) {
            EsCompany c = getCompany();
            OrgCompanyGeo geo = MybatisClient.getCompanyGeo(c.getOc_code());
            if (geo != null)
                c.setCoordinate(new EsGeoPoint() {{setLat(geo.latitude);setLon(geo.longitude);}});
        }
        if (getComstat() != null) {
            EsComStat s = getComstat();
        }
        return true;
    }
}
