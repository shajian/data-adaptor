package com.qianzhan.qichamao.task.stat;

import com.qianzhan.qichamao.collection.AdaFitHistogram;
import com.qianzhan.qichamao.entity.OrgCompanyStatisticsInfo;
import com.qianzhan.qichamao.util.MiscellanyUtil;
import com.qianzhan.qichamao.dal.mybatis.MybatisClient;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CompanyStatisticsInfo {
    private static AdaFitHistogram[] histograms;

    public static void initialize() {
        File file = new File("config/CompanyStatisticsInfo.bin");
        try {
            if (file.exists()) {
                System.out.println("trying to load company statistics info from file");
                if (deSerialize(file)) {
                    return;
                }
                System.out.println("failed to load company statistics info from file");
            }
            System.out.println("trying to load company statistics info from database");
            loadFromDB();
            serialize(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getScore(int[] counts) {
        if (counts == null || counts.length != histograms.length) return 0;
        int score = 0;
        for (int i = 0; i < histograms.length; ++i) {
            score += histograms[i].getScore(counts[i]);
        }
        return score;
    }

    private static void loadFromDB() throws Exception {
        List<AdaFitHistogram> list = new ArrayList<>();
        for (int i = 0; i < StatInfo.size.ordinal(); ++i) {
            list.add(new AdaFitHistogram(5));
        }
        histograms = list.toArray(new AdaFitHistogram[0]);
        // loading process must be finished without any interruption
        int start = 0;
        int end = loadInner(start);
        while (end > 0) {
            start = end;
            System.out.println(String.format("%s-checkpoint: %d @ %s",
                    Thread.currentThread().getStackTrace()[1].getClassName(),
                    start, new Date().toString()));
            end = loadInner(start);
        }

        for (int i = 0; i < histograms.length; ++i) {
            histograms[i].gen();
        }
    }

    private static int loadInner(int start) {
        List<OrgCompanyStatisticsInfo> infos = MybatisClient.getCompanyStatisticsInfos(start, 1000);
        int[][] counts = new int[histograms.length][infos.size()];
        for (int i = 0; i < infos.size(); ++i) {
            OrgCompanyStatisticsInfo info = infos.get(i);
            counts[StatInfo.share_holder.ordinal()][i] = info.GuDongXinXi;
            counts[StatInfo.senior_manager.ordinal()][i] = info.ZhuYaoRenYuan;
            counts[StatInfo.changes.ordinal()][i] = info.BianGengXinXi;
            counts[StatInfo.annual_report.ordinal()][i] = info.NianBao;
            counts[StatInfo.brand.ordinal()][i] = info.ShangBiaoXinXi;
            counts[StatInfo.patent.ordinal()][i] = info.ZhuanLiXinXi;
            counts[StatInfo.software.ordinal()][i] = info.RuanJianZhuZuoQuan;
            counts[StatInfo.product.ordinal()][i] = info.ZuoPinZhuZuoQuan;
            counts[StatInfo.domain.ordinal()][i] = info.YuMingBeiAn;
            counts[StatInfo.goods.ordinal()][i] = info.ShangPinTiaoMaXinXi;
            counts[StatInfo.factory.ordinal()][i] = info.ChangShangBianMaXinXi;
            counts[StatInfo.CNCA.ordinal()][i] = info.RenJianWei;
            counts[StatInfo.fire_station.ordinal()][i] = info.XiaoFangJu;
            counts[StatInfo.judge.ordinal()][i] = info.PanJueWenShu;
            counts[StatInfo.notice.ordinal()][i] = info.FaYuanGongGao;
            counts[StatInfo.executed.ordinal()][i] = info.BeiZhiXingRen;
            counts[StatInfo.dishonest.ordinal()][i] = info.ShiXinRen;
            counts[StatInfo.exhibit.ordinal()][i] = info.ZhanHuiHuiKan;
            counts[StatInfo.employ.ordinal()][i] = info.ZhaoPin;
            counts[StatInfo.branch.ordinal()][i] = info.FenZhi;
        }
        for (int i = 0; i < histograms.length; ++i) {
            histograms[i].adapt(counts[i]);
        }
        if (infos.size()>0)
            return infos.get(infos.size()-1).id;
        else
            return 0;
    }

    private static boolean deSerialize(File file) throws IOException {
        histograms = new AdaFitHistogram[StatInfo.size.ordinal()];
        FileInputStream fis = new FileInputStream(file);
        try {
            byte[] bytes = new byte[4];
            fis.read(bytes);
            int total = MiscellanyUtil.bytes2int(bytes);
            if (total == 0) return false;
            bytes = new byte[total];
            fis.read(bytes);

            int start = 0;
            for (int i = 0; i < histograms.length; ++i) {
                histograms[i] = new AdaFitHistogram();
                int length = histograms[i].deSerialize(bytes, start);
                start += length;
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fis.close();
        }
        return false;
    }

    private static void serialize(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);

        int total = 0;
        byte[][] bytes = new byte[histograms.length][];
        for (int i = 0; i < histograms.length; ++i) {
            bytes[i] = histograms[i].serialize();
            total += bytes[i].length;
        }
        try {
            fos.write(MiscellanyUtil.int2bytes(total));
            for (int i = 0; i < histograms.length; ++i) {
                fos.write(bytes[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fos.close();
        }
    }

    public enum StatInfo {
        share_holder,
        senior_manager,
        changes,
        annual_report,
        brand,
        patent,
        software,
        product,
        domain,
        goods,
        factory,
        CNCA,
        fire_station,
        judge,
        notice,
        executed,
        dishonest,
        exhibit,
        employ,
        invest,
        branch,
        /**
         * this is a special enumeration which means the count of all enumerations
         */
        size
    }
}
