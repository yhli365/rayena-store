package tool.jdk;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.Test;

public class CalendarTest {

	@Test
	public void testDateTime() throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[] tests = new String[] { "2014-12-31 12:23:30",
				"2030-12-31 12:23:30", "2200-01-01 00:00:01" };
		System.out.println("Integer.MAX_VALUE = " + Integer.MAX_VALUE);
		for (String t : tests) {
			System.out.println("\n---------test = " + t);
			long ts = df.parse(t).getTime();
			System.out.println("ts = " + ts);
			long seconds = ts / 1000L;
			System.out.println("seconds = " + seconds + ", intable="
					+ (seconds < Integer.MAX_VALUE));
			System.out.println("seconds2 = " + (int) seconds);
		}
	}

	@Test
	public void testDayValue() throws ParseException {
		String[] tests = new String[] { "2014-12-31 12:23:30",
				"2014-01-01 00:00:01" };
		for (String t : tests) {
			System.out.println("\n---------test = " + t);
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar cdar = Calendar.getInstance();
			cdar.setTime(df.parse(t));
			int dayValue = cdar.get(Calendar.YEAR) * 10000
					+ (cdar.get(Calendar.MONTH) + 1) * 100
					+ cdar.get(Calendar.DATE);
			System.out.println("dayValue = " + dayValue);
		}
	}
}
