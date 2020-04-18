package com.qianzhan.qichamao.task.com;

import com.qianzhan.qichamao.dal.mybatis.MybatisClient;
import com.qianzhan.qichamao.entity.EsComStat;
import com.qianzhan.qichamao.entity.EsCompany;
import com.qianzhan.qichamao.entity.OrgCompanyContact;
import com.qianzhan.qichamao.util.MiscellanyUtil;

import java.util.ArrayList;
import java.util.List;

public class ComContact extends ComBase {
    public ComContact(String key) {
        super(key);
    }

    @Override
    public void run() {
        if (compack.e_com != null) {
            EsCompany c = compack.e_com;
            List<String> m_phones = new ArrayList<>();
            List<String> f_phones = new ArrayList<>();
            List<String> mails = new ArrayList<>();
            for (OrgCompanyContact contact : MybatisClient.getCompanyContacts(c.getOc_code())) {
                if (MiscellanyUtil.isBlank(contact.oc_contact)) continue;
                if (contact.oc_status != 1) continue;
                if (contact.oc_type == 1) {
                    m_phones.add(contact.oc_contact);
                } else if (contact.oc_type == 2) {
                    f_phones.add(contact.oc_contact);
                } else if (contact.oc_type == 5) {
                    mails.add(contact.oc_contact);
                }
            }
            c.setMobile_phones(m_phones);
            c.setFix_phones(f_phones);
            c.setMails(mails);
        }

        ComBase.latch.countDown();
    }
}
