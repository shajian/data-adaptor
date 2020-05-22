package com.qianzhan.qichamao.dal.arangodb;

import com.arangodb.ArangoDB;
import com.qianzhan.qichamao.util.DbConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;

public class ArangoClient {
    private static ArangoDB client = null;
    private static String version;
    private static byte _supportPrune;

    static {
        String host = DbConfigBus.getDbConfig_s("arango.host", "");
        int port = DbConfigBus.getDbConfig_i("arango.port", -1);
        String user = DbConfigBus.getDbConfig_s("arango.user", "");
        String pwd = DbConfigBus.getDbConfig_s("arango.pass", "");
        String version = DbConfigBus.getDbConfig_s("arango.version", "0.0.0");
        client = new ArangoDB.Builder().host(host, port)
                .user(user).password(pwd).build();


    }

    public static ArangoDB getClient() {
        return client;
    }

    public static boolean supportPrune() {
        if (_supportPrune == -1) return false;
        if (_supportPrune == 1) return true;
        boolean prune = canPrune();
        if (prune) _supportPrune = 1;
        else _supportPrune = -1;
        return prune;
    }
    private static boolean canPrune() {
        // PRUNE is supported when version >= 3.4.5
        if (MiscellanyUtil.isBlank(version)) return false;
        String[] segs = version.split(".");
        int major = Integer.parseInt(segs[0]);
        if (major > 3) return true;
        if (major < 3 || segs.length == 1) return false;

        int minor = Integer.parseInt(segs[1]);
        if (minor > 4) return true;
        if (minor < 4 || segs.length == 2) return false;

        int build_num = Integer.parseInt(segs[2]);
        if (build_num >= 5) return true;
        return false;
    }
}
