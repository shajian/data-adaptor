package com.qcm.task.com;

import com.qcm.es.EsCompanyEntity;
import com.qcm.entity.OrgCompanyGeo;
import com.qcm.dal.mybatis.MybatisClient;
import org.elasticsearch.common.geo.GeoPoint;

public class SubTaskComGeo extends SubTaskComBase {
    public SubTaskComGeo(TaskType key) {
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
