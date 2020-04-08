package com.qianzhan.qichamao.dal.es;

import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.util.EsConfigBus;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import lombok.Getter;
import lombok.Setter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class EsCompanyInput extends EsBaseInput<EsCompany> {
    /**
     * searching version
     * this field has a format of 'xxxx-xx-xx', means year-month-day
     */
    @Setter@Getter
    private String version;


    public EsCompanyInput() {
        super();
    }

    /**
     * information of caller
     * this is useful when looking through log for analyzing afterwards
     * it is advised to compose it with some meaning segments.
     * concatenate segments using connector '-', '|' or ','
     */
    private String caller;


    /**
     * original keyword, only preprocess it by trim()
     */
    @Getter
    private String keyword;     // originally keyword provided by user through input-text-box

    @Getter
    private Map<String, Integer> keywords;
    private static Set<String> shields;     // get shields of keyword

    /**
     * default is 0 which means a single keyword; if set to positive,
     * keyword can be composed of multi sub-keywords
     * which are separated by space-blank('\s') or ('+/-').
     * If separated by '\s', all sub-keywords are positive.
     * NOTICE:
     *  1.keyword must contains chinese/non-ascii characters, otherwise
     *      searching engine works under mode 0(single keyword).
     *  2. if separated by '+/-', no space blank can come out in keyword
     *  3. when in multi-mode, fields.length must be equal to 1
     * MODE:
     *  0: single-mode
     *  1: multi-mode for union. if keyword is separated by '+/-', this mode is converted to intersection automatically
     *  2: multi-mode for intersection
     */
    @Getter
    private byte multi_mode;
    /**
     * generic
     * block_match
     */
    private byte match_mode;
    private Set<String> fields;
    @Getter
    private static Set<String> def_compound_fields = new HashSet<String>() {
        {
            add("oc_name");
            add("oc_address");
            add("business");
        }
    };
    @Getter
    private static Set<String> def_simple_fields = new HashSet<String>() {
        {
            add("oc_code");
            add("legal_person");
            add("senior_managers");
            add("share_holders");
        }
    };

    /**
     * keyword separator type: 1->'\s'; 2->'+-'
     */
    @Getter
    private int sep_type;

    @Getter
    private Map<String, String[]> filters;

    /**
     * no search keyword, just concern with recently registered companies
     * if `recent` is true, all other search/filter condition are ignored
     */
    @Getter@Setter
    private boolean recent;
    @Getter
    private double lat;
    @Getter
    private double lon;

    public static Set<String> getShields() {
        if (shields != null) return shields;
        shields = new HashSet<>();
        Object value = EsConfigBus.get("company.shields");
        if (value instanceof String) {
            String str = (String) value;
            if (str.contains(".txt")) { // read shield txt file
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                            new FileInputStream(EsCompanyInput.class.getClassLoader().
                                    getResource("EsConfig.yaml").getPath())
                    ));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        shields.add(line);
                    }
                    reader.close();
                } catch (IOException e) {
                }
            } else {
                shields.add(str);
            }
        } else if (value instanceof List){
            for (String shield : (List<String>) value) {
                shields.add(shield);
            }
        }
        return shields;
    }

    public void addFilter(String field, String... multi_values) {
        if (filters == null) {
            filters = new HashMap<>();
        }
        filters.put(field, multi_values);
        if ("coordinate".equals(field)) {
            lat = Double.parseDouble(multi_values[0]);
            lon = Double.parseDouble(multi_values[1]);
        }
    }
    public void setCoordinateDistance(double... latlondist) {
        lat = latlondist[0];
        lon = latlondist[1];
        double dist = latlondist[2];
        if (filters == null) filters = new HashMap<>();
        filters.put("coordinate", new String[] {lat+"", lon+"", dist+"" });
    }

    public void setFields(String... fields) throws Exception {
        this.fields = new HashSet<>();
        String first_field = null;
        for (String field: fields) {
            if (!MiscellanyUtil.isBlank(field)) {
                this.fields.add(field);
                first_field = field;
            }
        }
        if (multi_mode > 0) {
            if (this.fields.size() != 1) {
                throw new Exception("in multi-mode, only one field should be specified.");
            }
        }
    }

    public Set<String> getFields() {
        if (multi_mode > 0) {
            if (MiscellanyUtil.isArrayEmpty(this.fields)) {
                this.fields = new HashSet<String>() {{
                    add("oc_name");
                }};
            }
        }
        return this.fields;
    }

    public void setKeyword(String keyword) throws Exception {
        setKeyword(keyword, (byte) 0);
    }
    public void setKeyword(String keyword, byte multi_mode) throws Exception {
        if (MiscellanyUtil.isBlank(keyword)) return;
        this.keyword = keyword.trim();
        if (this.keyword.length() == 1 || getShields().contains(this.keyword)) {
            throw new Exception(
                    String.format("keyword '%s' is too wide, please use more concrete words", this.keyword));
        }
        if (multi_mode > 0) {                  // multi-mode
            separate();
            if (sep_type == 2) {    // if separated by +/-, mode is converted to intersection automatically
                this.multi_mode = 2;
            } else {
                sep_type = 1;
                this.multi_mode = multi_mode;
            }
            if (this.fields != null && this.fields.size() > 0) {
                throw new Exception("in multi-mode, only one field should be specified.");
            }
        }
    }

    private void separate() throws Exception{
        StringBuilder sb = new StringBuilder();
        keywords = new HashMap<>();
        boolean end = true;
        int pre_sign = 1;
        int cur_sign = 1; // 1: +; -1: -
        for (int i = 0; i < this.keyword.length(); ++i) {
            char c = this.keyword.charAt(i);
            if (c == '+') {
                if (sep_type == 0) sep_type = 2;
                else if (sep_type == 1)
                    throw new Exception("keyword cannot separated by space and +/- at the same time");
                cur_sign = 1;
                end = true;
            } else if (c == '-') {
                if (sep_type == 0) sep_type = 2;
                else if (sep_type == 1)
                    throw new Exception("keyword cannot separated by space and +/- at the same time");
                cur_sign = -1;
                end = true;
            } else if (c == ' ' || c == '\t') {
                if (sep_type == 0) sep_type = 1;
                else if (sep_type == 2)
                    throw new Exception("keyword cannot separated by space and +/- at the same time");
                cur_sign = 1;
                end = true;
            } else {
                sb.append(c);
                end = false;
            }

            if (end) {
                String segment = sb.toString();
                if (segment.length() > 1 && !getShields().contains(segment)) {
                    keywords.put(segment, pre_sign);
                    sb.delete(0, sb.length());
                }
                pre_sign = cur_sign;
            }
        }
        if (sb.length() > 1) {
            String segment = sb.toString();
            if (!getShields().contains(segment)) {
                keywords.put(segment, pre_sign);
            }
        }
        if (this.keywords.size() > 4) {
            throw new Exception("in multi-mode, at most 4 keywords are supported");
        } else if (this.keywords.size() == 0) {
            throw new Exception("words are too wide, please use more concrete words");
        }
    }
}
