package com.qianzhan.qichamao.dal.mybatis;

import com.qianzhan.qichamao.dal.DbName;
import com.qianzhan.qichamao.entity.*;
import com.qianzhan.qichamao.entity.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MybatisClient {

    /**
     * because `factory` keeps a connection-pool, so
     * `session` can be closed after a transaction finishing,
     * and be reopened when a new transaction request is coming.
     * Mapper-like object such as `comMapper` is the same as `session`.
     */
    private static Map<DbName, SqlSessionFactory> factories = new HashMap<DbName, SqlSessionFactory>();
    static {
        try {
//            InputStream is = MybatisClient.class.getResourceAsStream("/mybatis-config.xml");
//            Reader reader = Resources.getResourceAsReader("mybatis-config.xml");

            SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
            for (DbName db :
                    DbName.values()) {
                InputStream is = Resources.getResourceAsStream("mybatis-config.xml");
                factories.put(db, builder.build(is, db.name()));
                is.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     *
     * @param start is initialized with 0, and updated by start+=count at each iteration
     * @param count
     * @return
     */
    public static List<OrgCompanyList> getCompanies(int start, int count) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        RetrieveRange range = new RetrieveRange();
        range.setCount(count);
        range.setStart(start);
        List<OrgCompanyList> list = mapper.getCompanies(start, count);
        session.close();
        return list;
    }

    public static int getCheckpoint(String key) {
        SqlSession session = factories.get(DbName.com).openSession();
        CommonMapper mapper = session.getMapper(CommonMapper.class);
        Integer i = mapper.getCheckpoint(key);
        session.close();
        if (i != null) return i;
        return -1;
    }

    public static void updateCheckpoint(String key, int value) {
        SqlSession session = factories.get(DbName.com).openSession();
        CommonMapper mapper = session.getMapper(CommonMapper.class);
        mapper.updateCheckpoint(key, value);
        session.commit();
        session.close();
    }
    public static void insertCheckpoint0(String key) {
        SqlSession session = factories.get(DbName.com).openSession();
        CommonMapper mapper = session.getMapper(CommonMapper.class);
        mapper.insertCheckpoint0(key);
        session.commit();
        session.close();
    }
    public static void insertCheckpoint(String key, int value) {
        SqlSession session = factories.get(DbName.com).openSession();
        CommonMapper mapper = session.getMapper(CommonMapper.class);
        mapper.insertCheckpoint(key, value);
        session.commit();
        session.close();
    }

    public static OrgCompanyDtl getCompanyDtl(String oc_code) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        OrgCompanyDtl dtl = mapper.getCompanyDtl(oc_code);
        session.close();
        return dtl;
    }



    public static List<OrgCompanyDtlMgr> getCompanyMembers(String oc_code) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDtlMgr> members = mapper.getCompanyMembers(oc_code);
        session.close();
        return members;
    }

    public static List<OrgCompanyDtlMgr> getCompanyMembersGsxt(String oc_code, String tail) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDtlMgr> members = mapper.getCompanyMembersGsxt(oc_code, tail);
        session.close();
        return members;
    }

    public static List<OrgCompanyDtlGD> getCompanyGDs(String oc_code) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDtlGD> members = mapper.getCompanyGDs(oc_code);
        session.close();
        return members;
    }
    public static List<OrgCompanyGsxtDtlGD> getCompanyGDsGsxt(String oc_code, String tail) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyGsxtDtlGD> members = mapper.getCompanyGDsGsxt(oc_code, tail);
        session.close();
        return members;
    }

    public static List<OrgCompanyContact> getCompanyContacts(String oc_code) {
        SqlSession session  = factories.get(DbName.ext).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyContact> contacts = mapper.getCompanyContacts(oc_code);
        session.close();
        return contacts;
    }

    public static List<String> getCompanyOldNames(String oc_code) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<String> contacts = mapper.getCompanyOldNames(oc_code);
        session.close();
        return contacts;
    }

    public static List<OrgCompanyIndustry> getCompanyIndustries(String oc_code, String tail) {
        SqlSession session  = factories.get(DbName.extension).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyIndustry> contacts = mapper.getCompanyIndustries(oc_code, tail);
        session.close();
        return contacts;
    }

    public static OrgCompanyGeo getCompanyGeo(String oc_code) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        OrgCompanyGeo contacts = mapper.getCompanyGeo(oc_code);
        session.close();
        return contacts;
    }

    public static List<String> getCompanyBrands(String oc_code, String tail) {
        SqlSession session  = factories.get(DbName.brand).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<String> contacts = mapper.getCompanyBrands(oc_code, tail);
        session.close();
        return contacts;
    }

    public static List<OrgCompanyTag> getCompanyTags(String oc_code) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyTag> contacts = mapper.getCompanyTags(oc_code);
        session.close();
        return contacts;
    }

    public static List<OrgBrowseLog> getBrowseLogs(int start, int count) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgBrowseLog> contacts = mapper.getBrowseLogs(start, count);
        session.close();
        return contacts;
    }

    public static int getBrowseCount(String oc_code) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        Integer count = mapper.getBrowseCount(oc_code);
        session.close();
        if (count == null) return -1;
        return count;
    }

    public static void updateBrowseCount(String oc_code, int count) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        mapper.updateBrowseCount(oc_code, count);
        session.commit();
        session.close();
    }

    public static void insertBrowseCount(String oc_code, int count) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        mapper.insertBrowseCount(oc_code, count);
        session.commit();
        session.close();
    }

    public static OrgCompanyStatisticsInfo getCompanyStatisticsInfo(String oc_code) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        OrgCompanyStatisticsInfo info = mapper.getCompanyStatisticsInfo(oc_code);
        session.close();
        return info;
    }

    public static List<OrgCompanyStatisticsInfo> getCompanyStatisticsInfos(int start, int count) {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyStatisticsInfo> infos = mapper.getCompanyStatisticsInfos(start, count);
        session.close();
        return infos;
    }

    public static List<RetrieveRange> getBrowseCounts(int start, int count) {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<RetrieveRange> counts = mapper.getBrowseCounts(start, count);
        session.close();
        return counts;
    }

    public static void truncateBrowseCount() {
        SqlSession session  = factories.get(DbName.app).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        mapper.truncateBrowseCount();
        session.commit();
        session.close();
    }

    public static List<String> getGsxtSubtableNames() {
        SqlSession session  = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<String> tables = mapper.getGsxtSubtableNames();
        session.close();
        return tables;
    }

    public static List<OrgCompanyDimBatch> getCompanyGDBatch(int start, int count) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDimBatch> gds = mapper.getCompanyGDBatch(start, count);
        session.close();
        return gds;
    }

    public static List<OrgCompanyDimBatch> getCompanyMemberBatch(int start, int count) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDimBatch> gds = mapper.getCompanyMemberBatch(start, count);
        session.close();
        return gds;
    }

    public static List<OrgCompanyDimBatch> getCompanyGDGsxtBatch(int start, int count, String tail) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDimBatch> gds = mapper.getCompanyGDGsxtBatch(start, count, tail);
        session.close();
        return gds;
    }

    public static List<OrgCompanyDimBatch> getCompanyMemberGsxtBatch(int start, int count, String tail) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDimBatch> gds = mapper.getCompanyMemberGsxtBatch(start, count, tail);
        session.close();
        return gds;
    }

    public static List<OrgCompanyDimBatch> getCompanyDtls(int start, int count) {
        SqlSession session = factories.get(DbName.com).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyDimBatch> dtls = mapper.getCompanyDtls(start, count);
        session.close();
        return dtls;
    }

    public static List<OrgCompanyContact> getCompanyContactBatch(int start, int count) {
        SqlSession session  = factories.get(DbName.ext).openSession();
        ComMapper mapper = session.getMapper(ComMapper.class);
        List<OrgCompanyContact> contacts = mapper.getCompanyContactBatch(start, count);
        session.close();
        return contacts;
    }
}
