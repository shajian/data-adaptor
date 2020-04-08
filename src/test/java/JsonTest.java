import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.qianzhan.qichamao.dal.es.EsCompanyInput;

import java.io.FileOutputStream;

public class JsonTest {
    public static void main(String[] args) {
        EsCompanyInput input = new EsCompanyInput();

        try {
            input.setKeyword("some keyword");
            String json = JSON.toJSONString(input, SerializerFeature.PrettyFormat);
            FileOutputStream fos = new FileOutputStream("tmp/company_search.json");
            fos.write(json.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
