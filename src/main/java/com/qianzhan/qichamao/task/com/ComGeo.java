package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.EsGeoPoint;
import com.qianzhan.qichamao.entity.OrgCompanyGeo;

public class ComGeo extends ComBase {
    public ComGeo(String key) {
        super(key);
    }
    @Override
    public Boolean call() throws Exception {
        if (compack.e_com != null) {
            EsCompany c = compack.e_com;
            OrgCompanyGeo geo = MybatisClient.getCompanyGeo(c.getOc_code());
            if (geo != null)
                c.setCoordinate(new EsGeoPoint() {{setLat(geo.latitude);setLon(geo.longitude);}});
        }
        return true;
    }
}
