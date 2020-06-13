package com.qianzhan.qichamao.dal.mybatis;

import com.qianzhan.qichamao.entity.*;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ComMapper {
    List<OrgCompanyList> getCompanies(@Param("start") int start, @Param("count") int count);
    List<OrgCompanyDimBatch> getCompanyDtls(@Param("start") int start, @Param("count") int count);
    OrgCompanyDtl getCompanyDtl(String oc_code);
    List<OrgCompanyDtlMgr> getCompanyMembers(String oc_code);
    List<OrgCompanyDtlGD> getCompanyGDs(String oc_code);
    List<OrgCompanyDimBatch> getCompanyGDBatch(@Param("start") int start, @Param("count") int count);
    List<OrgCompanyDimBatch> getCompanyMemberBatch(@Param("start") int start, @Param("count") int count);
    List<OrgCompanyDtlMgr> getCompanyMembersGsxt(@Param("oc_code") String oc_code, @Param("tail") String tail);
    List<OrgCompanyGsxtDtlGD> getCompanyGDsGsxt(@Param("oc_code") String oc_code, @Param("tail") String tail);
    List<OrgCompanyDimBatch> getCompanyMemberGsxtBatch(@Param("start") int start, @Param("count") int count, @Param("tail") String tail);
    List<OrgCompanyDimBatch> getCompanyGDGsxtBatch(@Param("start") int start, @Param("count") int count, @Param("tail") String tail);
    List<OrgCompanyContact> getCompanyContacts(String oc_code);
    List<String> getCompanyOldNames(String oc_code);
    List<OrgCompanyIndustry> getCompanyIndustries(@Param("oc_code") String oc_code, @Param("tail") String tail);
    OrgCompanyGeo getCompanyGeo(String oc_code);
    List<String> getCompanyBrands(@Param("oc_code") String oc_code, @Param("tail") String tail);
    List<OrgCompanyTag> getCompanyTags(String oc_code);
    List<OrgBrowseLog> getBrowseLogs(@Param("start") int start, @Param("count") int count);
    void updateBrowseCount(@Param("oc_code") String oc_code, @Param("count") int count);
    void insertBrowseCount(@Param("oc_code") String start, @Param("count") int count);
    Integer getBrowseCount(String oc_code);
    OrgCompanyStatisticsInfo getCompanyStatisticsInfo(String oc_code);
    List<OrgCompanyStatisticsInfo> getCompanyStatisticsInfos(@Param("start") int start, @Param("count") int count);
    List<RetrieveRange> getBrowseCounts(@Param("start") int start, @Param("count") int count);
    void truncateBrowseCount();

    List<String> getGsxtSubtableNames();

    List<OrgCompanyContact> getCompanyContactBatch(@Param("start") int start, @Param("count") int count);
}
