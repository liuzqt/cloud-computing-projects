import java.util.TimeZone;

/**
* @author ziqiLiu 
* @version date：2016年10月12日 下午4:04:05
* class description
*/
public class test {

	public static void main(String[] args) {
		final long timestamp = System.currentTimeMillis() + TimeZone.getTimeZone("EST").getRawOffset();
		System.out.println(timestamp);

		// TODO Auto-generated method stub

	}

}
