package com.qianzhan.qichamao.dal.arangodb;

import com.arangodb.ArangoDB;
import com.qianzhan.qichamao.util.DbConfigBus;
import lombok.Getter;

public class ArangoClient {
    private static ArangoDB client = null;

    static {
        String host = DbConfigBus.getDbConfig_s("arango.host", "");
        int port = DbConfigBus.getDbConfig_i("arango.port", -1);
        String user = DbConfigBus.getDbConfig_s("arango.user", "");
        String pwd = DbConfigBus.getDbConfig_s("arango.pass", "");
        client = new ArangoDB.Builder().host(host, port)
                .user(user).password(pwd).build();
    }

    public static ArangoDB getClient() {
        return client;
    }
}
