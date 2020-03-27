package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class EsComUtil {
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

}
