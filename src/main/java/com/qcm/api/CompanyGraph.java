package com.qcm.api;

import com.alibaba.fastjson.JSON;
import com.qcm.dal.arangodb.ArangoBusinessRepository;
import com.qcm.graph.GraphParam;
import com.qcm.graph.PersonAggregation;

import java.util.List;

public class CompanyGraph {
    public static String aggregate(GraphParam param) {
        try {
            if (param.code == null) {
                // check param validation
                List<PersonAggregation> aggs = ArangoBusinessRepository.singleton().aggregate(
                        param.person, param.start, param.count);
                return JSON.toJSONString(aggs, param.pretty);
            } else {
                PersonAggregation agg = ArangoBusinessRepository.singleton().aggregate(param.code, param.person);
                return JSON.toJSONString(agg, param.pretty);
            }
        } catch (Exception e) {
            // todo log
            return e.getMessage();
        }
    }
}
