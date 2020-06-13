package com.qianzhan.qichamao.dal.hbase;

import com.qianzhan.qichamao.entity.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class HbaseCompanyRepository extends HbaseRepository<HbaseCompany> {
    public HbaseCompany[] get_s(String[] keys) throws Exception {
        Result[] rs = get(keys, new String[]{"s"});
        HbaseCompany[] companies = new HbaseCompany[rs.length];
        for (int i = 0; i < rs.length; ++i) {
            companies[i] = from_s(rs[i]);
        }
        return companies;
    }

    private HbaseCompany from_s(Result r) throws Exception {
        HbaseCompany company = new HbaseCompany();
        company.setOc_code(Bytes.toString(r.getRow()));
        byte[] f = Bytes.toBytes("s");
        byte[] c = Bytes.toBytes("oc_name");
        if (r.containsColumn(f, c))
            company.setOc_name(Bytes.toString(r.getValue(f, c)));

        c = Bytes.toBytes("oc_number");
        if (r.containsColumn(f, c))
            company.setOc_number(Bytes.toString(r.getValue(f, c)));

        c = Bytes.toBytes("oc_area");
        if (r.containsColumn(f, c))
            company.setOc_area(Bytes.toString(r.getValue(f, c)));

        c = Bytes.toBytes("credit_code");
        if (r.containsColumn(f, c))
            company.setCredit_code(Bytes.toString(r.getValue(f, c)));

        c = Bytes.toBytes("establish_date");
        if (r.containsColumn(f, c))
            company.setEstablish_date(new Date(Bytes.toLong(r.getValue(f, c))));

        c = Bytes.toBytes("legal_person");
        if (r.containsColumn(f, c)) {
            Cell cell = r.getColumnLatestCell(f,c);
            HbaseVersion<HbaseString> lp = new HbaseVersion<>();
            lp.setM(false);
            lp.setVersion(new Date(cell.getTimestamp()));
            lp.fromBytes(cell.getValueArray());
            company.setLegal_person(lp);
        }

        c = Bytes.toBytes("share_holders");
        if (r.containsColumn(f, c)) {
            Cell cell = r.getColumnLatestCell(f,c);
            HbaseVersion<HbaseComInvest> sh = new HbaseVersion<>();
            sh.setM(true);
            sh.setVersion(new Date(cell.getTimestamp()));
            sh.setTs(new HbaseMulti<HbaseComInvest>(){{setSeed(new HbaseComInvest(){{setIn(true);}});}});
            sh.fromBytes(cell.getValueArray());
            company.setShare_holders(sh);
        }

        c = Bytes.toBytes("senior_managers");
        if (r.containsColumn(f, c)) {
            Cell cell = r.getColumnLatestCell(f,c);
            HbaseVersion<HbaseComPosition> sm = new HbaseVersion<>();
            sm.setM(true);
            sm.setVersion(new Date(cell.getTimestamp()));
            sm.fromBytes(cell.getValueArray());
            company.setSenior_managers(sm);
        }

        c = Bytes.toBytes("invests");
        if (r.containsColumn(f, c)) {
            Cell cell = r.getColumnLatestCell(f,c);
            HbaseVersion<HbaseComInvest> inv = new HbaseVersion<>();
            inv.setM(true);
            inv.setVersion(new Date(cell.getTimestamp()));
            inv.fromBytes(cell.getValueArray());
            company.setInvests(inv);
        }

        return company;
    }

    private HbaseCompany from_m(Result r) throws Exception {
        HbaseCompany company = new HbaseCompany();
        company.setOc_code(Bytes.toString(r.getRow()));
        byte[] f = Bytes.toBytes("m");
        byte[] c = Bytes.toBytes("p_sh");
        Map<Long, HbaseDiffVersion<HbaseComInvest>> map = new TreeMap<>();
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComInvest> sh = new HbaseDiffVersion<>();
                sh.setDate(new Date(cell.getTimestamp()));
                HbaseMulti<HbaseComInvest> pt = new HbaseMulti<>();
                pt.setSeed(new HbaseComInvest(){{setIn(true);}});
                pt.fromBytes(cell.getValueArray());
                sh.setPt(pt);
                map.put(cell.getTimestamp(), sh);
            }
        }
        c = Bytes.toBytes("n_sh");
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComInvest> sh = map.get(cell.getTimestamp());
                if (sh == null) {
                    sh = new HbaseDiffVersion<>();
                    sh.setDate(new Date(cell.getTimestamp()));
                    map.put(cell.getTimestamp(), sh);
                }
                HbaseMulti<HbaseComInvest> nt = new HbaseMulti<>();
                nt.setSeed(new HbaseComInvest(){{setIn(true);}});
                nt.fromBytes(cell.getValueArray());
                sh.setNt(nt);
            }
        }
        // construct HbaseVersions
        if (map.size()>0) {
            HbaseVersions<HbaseComInvest> diff_sh = new HbaseVersions<>();
            diff_sh.setDiffs((HbaseDiffVersion<HbaseComInvest>[]) map.values().toArray());
            company.setDiff_sh(diff_sh);
        }

        c = Bytes.toBytes("p_inv");
        map.clear();
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComInvest> inv = new HbaseDiffVersion<>();
                inv.setDate(new Date(cell.getTimestamp()));
                HbaseMulti<HbaseComInvest> pt = new HbaseMulti<>();
                pt.fromBytes(cell.getValueArray());
                inv.setPt(pt);
                map.put(cell.getTimestamp(), inv);
            }
        }
        c = Bytes.toBytes("n_inv");
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComInvest> sh = map.get(cell.getTimestamp());
                if (sh == null) {
                    sh = new HbaseDiffVersion<>();
                    sh.setDate(new Date(cell.getTimestamp()));
                    map.put(cell.getTimestamp(), sh);
                }
                HbaseMulti<HbaseComInvest> nt = new HbaseMulti<>();
                nt.fromBytes(cell.getValueArray());
                sh.setNt(nt);
            }
        }
        // construct HbaseVersions
        if (map.size()>0) {
            HbaseVersions<HbaseComInvest> diff_inv = new HbaseVersions<>();
            diff_inv.setDiffs((HbaseDiffVersion<HbaseComInvest>[]) map.values().toArray());
            company.setDiff_inv(diff_inv);
        }
        map.clear();

        c = Bytes.toBytes("p_sm");
        Map<Long, HbaseDiffVersion<HbaseComPosition>> sms = new TreeMap<>();
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComPosition> sm = new HbaseDiffVersion<>();
                sm.setDate(new Date(cell.getTimestamp()));
                HbaseMulti<HbaseComPosition> pt = new HbaseMulti<>();
                pt.fromBytes(cell.getValueArray());
                sm.setPt(pt);
                sms.put(cell.getTimestamp(), sm);
            }
        }
        c = Bytes.toBytes("n_sm");
        if (r.containsColumn(f, c)) {
            List<Cell> cells = r.getColumnCells(f, c);
            for (Cell cell : cells) {
                HbaseDiffVersion<HbaseComPosition> sm = sms.get(cell.getTimestamp());
                if (sm == null) {
                    sm = new HbaseDiffVersion<>();
                    sm.setDate(new Date(cell.getTimestamp()));
                    sms.put(cell.getTimestamp(), sm);
                }
                HbaseMulti<HbaseComPosition> nt = new HbaseMulti<>();
                nt.fromBytes(cell.getValueArray());
                sm.setNt(nt);
            }
        }
        // construct HbaseVersions
        if (sms.size()>0) {
            HbaseVersions<HbaseComPosition> diff_sm = new HbaseVersions<>();
            diff_sm.setDiffs((HbaseDiffVersion<HbaseComPosition>[]) map.values().toArray());
            company.setDiff_sm(diff_sm);
        }
        return company;
    }
}
