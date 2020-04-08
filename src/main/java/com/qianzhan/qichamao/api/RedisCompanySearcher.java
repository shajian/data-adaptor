package com.qianzhan.qichamao.api;

import com.qianzhan.qichamao.dal.RedisClient;

import java.util.*;

public class RedisCompanySearcher {
    public static String[] fname2code(String[] names) {
        List<String> rs = RedisClient.get(RedisClient.reverseIndexDb).mget(names);
        String[] codes = new String[names.length];
        Map<String, Integer> setKeys = new HashMap<>();
        int i = 0;
        for (String r : rs) {
            if (r == null) {
                setKeys.put(String.format("s:%s", names[i]), i);
            } else {
                codes[i] = r;
            }
            i++;
        }
        for (String k : setKeys.keySet()) {
            Set<String> vs = RedisClient.get(RedisClient.reverseIndexDb).smembers(k);
            if (vs.size() > 0) {
                codes[setKeys.get(k)] = String.join(",", vs);
            }
        }
        return codes;
    }
}
