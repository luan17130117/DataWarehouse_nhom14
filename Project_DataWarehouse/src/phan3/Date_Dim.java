package phan3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.Months;
import org.joda.time.Period;

import connection.database.GetConnection;

// Load Date_Dim vào Warehouse
public class Date_Dim {
	// Tạo số lượng record
	public static final int NUMBER_OF_RECORD = 1000;
	
	//
	public static void main(String[] args) throws SQLException {
		int stt = 0;
		int sk_date = 0;
		int month_since_1999 = 1;
		int day_since_1999 = 0;
		int quarter_since_1999_temp = 0;
		int quarter_temp = 1;
		// Kết nối đếm Data Warehouse
		Connection connection = new GetConnection().getConnection("warehouse");
		// Ngày bắt đầu
		DateTime startDateTime = new DateTime(1998, 12, 31, 0, 0, 0);
		DateTime startDateTimeforMonth = startDateTime.plus(Period.days(1));
		// Tạo dữ liệu insert vào data warehouse
		while (stt <= NUMBER_OF_RECORD) {
			startDateTime = startDateTime.plus(Period.days(1));
			Date startDate = startDateTime.toDate();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(startDate);
			// SK_Date
			sk_date += 1; // 1
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
			// Ngày tháng năm
			String full_date = dt.format(calendar.getTime()); // 2
			// Ngày từ năm 1999
			day_since_1999 += 1; // 3
			int month_since_1999_temp = Months
					.monthsBetween(startDateTimeforMonth.toLocalDate(), startDateTime.toLocalDate()).getMonths();
			// tháng từ năm 1999
			month_since_1999 = month_since_1999_temp + 1; //4
			// Thứ trong tuần
			String day_of_week = calendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.US); // 5
			// Lịch tháng
			String calendar_month = calendar.getDisplayName(Calendar.MONTH, Calendar.LONG, Locale.US); // 6
			dt = new SimpleDateFormat("yyyy");
			// Lịch năm
			String calendar_year = dt.format(calendar.getTime()); // 7
			String calendar_month_short = calendar.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);
			// Lịch năm tháng
			String calendar_year_month = calendar_year + "-" + calendar_month_short; // 8
			// Ngày của tháng
			int day_of_month = calendar.get(Calendar.DAY_OF_MONTH); // 9
			// Ngày của năm
			int day_of_year = calendar.get(Calendar.DAY_OF_YEAR); // 10
			Calendar calendar_temp = calendar;
			// Ngày chủ nhật của năm
			int week_of_year_sunday = lastDayOfLastWeek(calendar_temp).get(Calendar.WEEK_OF_YEAR); // 11
			int year_sunday = lastDayOfLastWeek(calendar).get(Calendar.YEAR);
			// Ngày chủ nhật của tháng
			String year_week_sunday = ""; // 12
			if (week_of_year_sunday < 10) {
				year_week_sunday = year_sunday + "-" + "W0" + week_of_year_sunday;
			} else {
				year_week_sunday = year_sunday + "-" + "W" + week_of_year_sunday;
			}
			calendar_temp = Calendar.getInstance(Locale.US);
			calendar_temp.setTime(calendar.getTime());
			calendar_temp.set(Calendar.DAY_OF_WEEK, calendar_temp.getFirstDayOfWeek());
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Tuần chủ nhật bắt đầu
			String week_sunday_start = dt.format(calendar_temp.getTime()); // 13
			DateTime startOfWeek = startDateTime.weekOfWeekyear().roundFloorCopy();
			// Tuần chủ nhật của tuần năm
			int week_of_year_monday = startOfWeek.getWeekOfWeekyear(); // 14
			dt = new SimpleDateFormat("yyyy");
			int year_week_monday_temp = startOfWeek.getYear();
			// Thứ 2 của tuần năm
			String year_week_monday = "";
			if (week_of_year_monday < 10) {
				year_week_monday = year_week_monday_temp + "-W0" + week_of_year_monday;
			} else {
				year_week_monday = year_week_monday_temp + "-W" + week_of_year_monday;
			}
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Bắt đầu thứ 2 của năm
			String week_monday_start = dt.format(startOfWeek.toDate()); // 16
			// Qúy của năm 1999
			int month = startDateTime.getMonthOfYear();
			int quarter = month % 3 == 0 ? (month / 3) : (month / 3) + 1;
			if (quarter == quarter_temp) {
				quarter_since_1999_temp = quarter_since_1999_temp + 1;
				quarter_temp += 1;
				if (quarter_temp > 4) {
					quarter_temp = 1;
				}
			}

			int quarter_since_1999 = 0;
			quarter_since_1999 += quarter_since_1999_temp; //17

			// Qúy của tháng trong năm
			String quarter_year = startDateTime.getYear() + "";
			String quarter_of_year_temp = getQuarter(startDateTime.getMonthOfYear());
			String quarter_of_year = quarter_year + "-" + quarter_of_year_temp; //18
			// Ngày nghỉ
			String holiday = "Non-Holiday"; // 19
			// Loại ngày
			String day_type = isWeekend(day_of_week); // 20
//			String output = sk_date + "," + full_date + "," + day_since_2009 + "," + month_since_2009 + ","
//					+ day_of_week + "," + calendar_month + "," + calendar_year + "," + calendar_year_month + ","
//					+ day_of_month + "," + day_of_year + "," + week_of_year_sunday + "," + year_week_sunday + ","
//					+ week_sunday_start + "," + week_of_year_monday + "," + year_week_monday + "," + week_monday_start
//					+ "," + holiday + "," + day_type+","+quarter_of_year+","+quarter_since_2009;
//			System.out.println(output);
			
			// bảng date_dim trong database_warehouse
			String datedim = "Insert into date_dim values ('"+sk_date + "','" + full_date + "','" + day_since_1999 + "','" + month_since_1999 + "','"
					+ day_of_week + "','" + calendar_month + "','" + calendar_year + "','" + calendar_year_month + "','"
					+ day_of_month + "','" + day_of_year + "','" + week_of_year_sunday + "','" + year_week_sunday + "','"
					+ week_sunday_start + "','" + week_of_year_monday + "','" + year_week_monday + "','" + week_monday_start
					+ "','" + holiday + "','" + day_type+"','"+quarter_of_year+"','"+quarter_since_1999+"') ";
			PreparedStatement ps = connection.prepareStatement(datedim);
			ps.executeUpdate(datedim);
			stt++;
		}
	}

	public static String getWeekOfYearSunday(Calendar calendar) {
		Date date = getFirstDayOfWeekDate(calendar);
		Calendar newCalendar = Calendar.getInstance(Locale.US);
		newCalendar.setTime(date);
		int result = newCalendar.getWeeksInWeekYear();
		return "" + result;
	}

	public static String getFirstDayOfWeekString(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		String result = dt.format(temp);
		return result;
	}

	public static Date getFirstDayOfWeekDate(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		return temp;
	}

	public static Calendar getDateOfMondayInCurrentWeek(Calendar c) {
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int today = c.get(Calendar.DAY_OF_WEEK);
		c.add(Calendar.DAY_OF_WEEK, -today + Calendar.MONDAY);
		return c;
	}

	public static Calendar firstDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// Tuần trước
		c.add(Calendar.WEEK_OF_YEAR, -1);
		// Ngày đầu
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		return c;
	}

	public static Calendar lastDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// Ngày đầu tuần này
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		// Ngày cuối tuần trước
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c;
	}

	// Kiểm tra Ngày đã cho là cuối tuần (Thứ bảy hoặc Chủ nhật)
	public static String isWeekend(String day) {
		if (day.equalsIgnoreCase("Saturday") || day.equalsIgnoreCase("Sunday")) {
			return "Weekend";
		} else {
			return "Weekday";
		}
	}

	// Tính quý của tháng
	public static String getQuarter(int month) {
		int quarter = month % 3 == 0 ? (month / 3) : (month / 3) + 1;
		String result = "Q" + quarter;
		return result;
	}
}
