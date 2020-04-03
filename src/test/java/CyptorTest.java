import com.qianzhan.qichamao.util.Cryptor;

public class CyptorTest {
    public static void main(String[] args) {
        String origin = "脱贫攻坚";
        String origin2 = "Maven项目对象模型(POM)，可以通过一小段描述信息来管理项目的";
        String encrypted = Cryptor.encrypt_des(origin);
        System.out.println(encrypted);
        String encrypted2 = Cryptor.encrypt_des(origin2);
        System.out.println(encrypted2);
        String decrypted = Cryptor.decrypt_des(encrypted);
        String decrypted2 = Cryptor.decrypt_des(encrypted2);
        System.out.println(String.format("%d, %d", origin.getBytes().length, encrypted.getBytes().length));
        System.out.println(String.format("%d, %d", origin2.getBytes().length, encrypted2.getBytes().length));
        System.out.println("origina: " + origin + " , decrypted: " + decrypted);
        System.out.println("origina: " + origin2 + " , decrypted: " + decrypted2);
    }
}
