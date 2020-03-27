package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.util.EsConfigBus;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class EsIndexSetting {
    private int shards;
    private int replicas;
    private Map<String, String> analyzers;

    public EsIndexSetting set(String index) {
        String main = index.split("_")[0];
        analyzers = EsConfigBus.get(main + ".analyzers");
        Integer s = EsConfigBus.get(main+".shards");
        Integer r = EsConfigBus.get(main + ".replicas");
        if (s != null)
            shards = s;
        if (r != null)
            replicas = r;
        return this;
    }
}
