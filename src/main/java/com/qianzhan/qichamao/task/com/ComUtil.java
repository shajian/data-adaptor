package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.config.GlobalConfig;
import com.qianzhan.qichamao.dal.es.EsCompanyInput;
import com.qianzhan.qichamao.dal.es.EsCompanyRepository;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.api.EsCompanySearcher;
import com.qianzhan.qichamao.dal.RedisClient;
import com.qianzhan.qichamao.entity.EsCompanyTripleMatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ComUtil {
    public static byte getCompanyStatus(String ext) {
        if (MiscellanyUtil.isBlank(ext)) return 0;  // unknown
        if (ext.contains("存续")) return 2;
        if (ext.contains("迁入")) return 10;
        if (ext.contains("迁出")) return 3;
        if (ext.contains("吊销")) return 4;
        if (ext.contains("注销") || ext.contains("非正常")) return 5;
        if (ext.contains("停业")) return 6;
        if (ext.contains("解散")) return 7;
        if (ext.contains("清算")) return 8;
        if (ext.contains("撤销")) return 11;
        if (ext.contains("在营") || ext.contains("正常") || ext.contains("开业") || ext.contains("确立")
                || ext.contains("在业") || ext.contains("在册") || ext.contains("登记成立")) return 1;
        return 0;
    }

    public static boolean isCompanyStatusNormal(byte status) {
        return status == 1 || status == 2 || status == 10;
    }

    public static String getCompanyStatus(int i) {
        if (i == 0) return "其他";
        if (i == 1) return "在业";
        if (i == 2) return "存续";
        if (i == 3) return "迁出";
        if (i == 4) return "吊销";
        if (i == 5) return "注销";
        if (i == 6) return "停业";
        if (i == 7) return "解散";
        if (i == 8) return "清算";
        if (i == 10) return "迁入";
        if (i == 11) return "撤销";
        return "其他";
    }

    public static List<String> getCompanyType(String regType, String oc_name) {
        List<String> types = new ArrayList<>(2);
        if (!MiscellanyUtil.isBlank(regType)) {
            if ((regType.contains("个体") || regType.contains("个体工商户"))
                    && !oc_name.endsWith("公司")) {
                types.add("个体工商户");
            }
            if (regType.contains("合伙")) {  // 合伙人制度
                types.add("合伙制企业");
            }
            if (regType.contains("独资")) {
                types.add("独资企业");
            }
            if (regType.contains("股份")) {     // 股份制企业是国企吗
                types.add("股份制企业");
            }
            if (regType.contains("股份有限公司")) {   // 有限公司和有限责任公司的区别
                types.add("股份有限公司");
            }
            if (regType.contains("有限责任")) {
                types.add("有限责任公司");
            }
            if (regType.contains("国有") && !regType.contains("非国有")) {
                types.add("国企");
            }
            if (regType.contains("外商") && !regType.contains("合资")) {     // 外商独资企业与中外合资企业
                types.add("外企");
            }
        }
        if (types.isEmpty()) {
            types.add("其他");
        }
        return types;
    }

    /**
     * By 2017 National Industry Category
     * @param code
     * @return
     */
    public static String getMainIndustry(int code) {
        if (code < 1 || code > 97) return null;
        if (code <= 5) return "A";
        if (code <= 12) return "B";
        if (code <= 43) return "C";
        if (code <= 46) return "D";
        if (code <= 50) return "E";
        if (code <= 52) return "F";
        if (code <= 60) return "G";
        if (code <= 62) return "H";
        if (code <= 65) return "I";
        if (code <= 69) return "J";
        if (code == 70) return "K";
        if (code <= 72) return "L";
        if (code <= 75) return "M";
        if (code <= 79) return "N";
        if (code <= 82) return "O";
        if (code == 83) return "P";
        if (code <= 85) return "Q";
        if (code <= 90) return "R";
        if (code <= 96) return "S";
        return "T";
    }


    public static List<String> getCodeAreas(String oc_name) {
        List<String> codeAreas = new ArrayList<>();
        if (MiscellanyUtil.isBlank(oc_name)) return codeAreas;
        String codearea = RedisClient.get(oc_name);
        if (!MiscellanyUtil.isBlank(codearea)) {
            codeAreas.add(codearea);
        } else {
            Set<String> codeareas = RedisClient.smembers("s:" + oc_name);

            if (!MiscellanyUtil.isArrayEmpty(codeareas)) {
                String[] codes = new String[codeareas.size()];
                int i = 0;
                for (String ca : codeareas) {
                    codes[i] = ca.substring(0,9);
                    i++;
                }
                EsCompanyInput input = new EsCompanyInput();
                input.setIds(codes);
                input.setSrc_inc("oc_code", "oc_status", "oc_area");
                List<EsCompany> companies = EsCompanyRepository.singleton().mget(input);
                for (EsCompany company : companies) {
                    if (isCompanyStatusNormal(company.getOc_status())) {
                        codeAreas.add(company.getOc_code()+company.getOc_area());
                    }
                }
            } else {
                // search from ES
                try {
                    List<EsCompanyTripleMatch> matches = EsCompanySearcher.name2code(oc_name);
                    String backup = null;
                    for (EsCompanyTripleMatch match : matches) {
                        if (match.getConfidence() > 0.8) {
                            if (backup == null) {
                                backup = match.getOc_code() + match.getOc_area();
                            }

                            byte status = match.getOc_status();
                            if (isCompanyStatusNormal(status)) {
                                codeAreas.add(match.getOc_code() + match.getOc_area());
                            }
                        }
                    }
                    if (codeAreas.isEmpty() && backup != null) { //
                        codeAreas.add(backup);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return codeAreas;
    }

    /**
     * remove noise information in a company name.
     * this methods must be used at very last step.
     * @param oc_name
     * @return
     */
    public static String pruneCompanyName(String oc_name) {
        if (MiscellanyUtil.isBlank(oc_name)) return null;
        oc_name = oc_name.trim();
        int idx = oc_name.indexOf("公司");
        if (idx >= 6) {
            char c = oc_name.charAt(idx+2);
            if (c == '(' || c == '（') {
                c = oc_name.charAt(oc_name.length() - 1);
                if (c == ')' || c == '）') {
                    return oc_name.substring(0, idx+2);
                }
            }
        } else {
            idx = oc_name.lastIndexOf("(");
            if (idx < 6) {
                idx = oc_name.lastIndexOf("（");
            }
            if (idx >= 6) {
                char c = oc_name.charAt(oc_name.length() - 1);
                if (c == ')' || c == '）') {
                    return oc_name.substring(0, idx);
                }
            }
        }
        return oc_name;
    }

    /**
     * get the distance between `from` and `to` vertices
     * `to` is a company, while `from` is its legal person, share holder or senior member.
     * the distance is depended on the count of same type `from`.
     * @param outDegree
     * @return
     */
    public static int edgeLength(int outDegree) {
        if (outDegree <= 0) return 1;   // default
        return 1 + outDegree/ArangodbCompanyWriter.dist_step;
    }
}
