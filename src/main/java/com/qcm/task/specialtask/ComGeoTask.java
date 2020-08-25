package com.qcm.task.specialtask;

import com.qcm.es.entity.EsCompanyEntity;
import com.qcm.entity.OrgCompanyGeo;
import com.qcm.dal.mybatis.MybatisClient;
import com.qcm.task.maintask.TaskType;
import org.elasticsearch.common.geo.GeoPoint;

public class ComGeoTask extends BaseTask {
    public ComGeoTask(TaskType key) {
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
