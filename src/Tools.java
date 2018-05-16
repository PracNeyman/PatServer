import java.util.Arrays;
import java.util.List;

public class Tools {
    //list转化为str
    public static String listToString(List<String> list){
        if(list==null)
            return null;
        StringBuilder rs = new StringBuilder();
        boolean first = true;
        for(String str:list){
            //第一个字符串值钱不拼接逗号
            if(first) {
                first = false;
            }
            else {
                rs.append(",");
            }
            rs.append(str);
        }
        return rs.toString();
    }

    //str转化为list
    public static List<String> stringToList(String str){
        //拿逗号分隔开连接的字符串
        String[] strs = str.split(",");
        return Arrays.asList(strs);
    }
}
