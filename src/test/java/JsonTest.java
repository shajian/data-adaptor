import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.qianzhan.qichamao.dal.es.EsCompanyInput;

import java.io.FileOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonTest {
    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("[^a-zA-Z\\u4e00-\\u9fa5]+");
        String[] companies = new String[] {
            "abc.",
            "010",
            ".@#$",
            "你好-（）",
            "abcdsegt435"
        };
        for (String company : companies) {
            Matcher matcher = pattern.matcher(company);
            if (matcher.matches()) {
                System.out.println(company + " match");
            } else {
                System.out.println(company + " unmatch");
            }
        }
//        EsCompanyInput input = new EsCompanyInput();
//
//        try {
//            input.setKeyword("some keyword");
//            String json = JSON.toJSONString(input, SerializerFeature.PrettyFormat);
//            FileOutputStream fos = new FileOutputStream("tmp/company_search.json");
//            fos.write(json.getBytes());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
