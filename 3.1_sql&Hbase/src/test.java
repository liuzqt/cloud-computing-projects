import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;

/**
* @author ziqiLiu 
* @version date：2016年10月7日 上午12:26:56
* class description
*/
public class test {

	public static void main(String[] args) {
		String string = "0.4111629833";
//		Pattern pattern = Pattern.compile("(\\d{2,9}|[4-9])\\d{2}\\.\\d+");
		Pattern pattern = Pattern.compile("(\\d{2,9}|[1-9])(\\.\\d+|)");
		Matcher matcher = pattern.matcher(string);
		if(matcher.matches()){
			System.out.println("match!");
		}
		
	}

}
