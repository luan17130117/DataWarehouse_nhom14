package phan2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import connection.database.GetConnection;

//Step2: Load data file from local to DB Staging
public class Staging {

	// tra ve danh sach cac file da tai ve tu SCP va san sang load vao staging
	public static ArrayList<HashMap<String, String>> getFileInControl() {
		ArrayList<HashMap<String, String>> lst = new ArrayList<>();
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// 1. Kết nối tới DB control
			conn = new GetConnection().getConnection("control");
			// 2. Tìm tất cả các file có trạng thái OK download và ở các nhóm đang active
			pre_control = conn.prepareStatement("SELECT data_file_logs.id ,id_config, your_filename, table_staging, "
					+ " data_file_configuaration.delimiter, data_file_configuaration.localPath,"
					+ "data_file_configuaration.number_column from data_file_logs "
					+ "JOIN data_file_configuaration ON data_file_logs.id_config = data_file_configuaration.id"
					+ " where "
					+ "data_file_logs.status_file like 'Download ok' AND data_file_configuaration.isActive=1 ");
			// 3. Nhận được ResultSet chứa các record thỏa điều kiện truy xuất
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng dòng record và lấy các giá trị put vào Map
			while (re.next()) {
				HashMap<String, String> hm = new HashMap<String, String>();
				hm.put("id", Integer.toString(re.getInt("id")));
				hm.put("encode", "UTF-8");
//				hm.put("values", re.getString("insert_staging"));
				hm.put("table_staging", re.getString("table_staging"));
				hm.put("dir", re.getString("localPath"));
				hm.put("filename", re.getString("your_filename"));
				hm.put("delimiter", re.getString("delimiter"));
				hm.put("number_column", Integer.toString(re.getInt("number_column")));
				lst.add(hm);
				System.out.println(hm);
			}
			// 5.1. Đóng kết nối DB control
			re.close();
			pre_control.close();
			conn.close();
			// 5.2. Trả về danh sách các file
			return lst;
		} catch (Exception e) {
			return lst;
		}
	}

	// load sinh vien vao db staging theo tung file
	public static void loadStudentToStaging() {
		ArrayList<HashMap<String, String>> lst = getFileInControl();
		System.out.println(lst.size());
		// 6. Duyệt danh sách các file
		for (int i = 0; i < lst.size(); i++) {
			String fileNamei = lst.get(i).get("filename");// abc.txt
			String fileNameo = fileNamei.substring(0, fileNamei.lastIndexOf("."));
			String path = lst.get(i).get("dir") + File.separator + fileNamei;
			File file = null;
			String pathConvert = lst.get(i).get("dir") + File.separator + fileNameo + ".txt";
			////
			if (fileNamei.substring(fileNamei.lastIndexOf(".") + 1).equalsIgnoreCase("xlsx")
					|| fileNamei.substring(fileNamei.lastIndexOf(".") + 1).equalsIgnoreCase("xls")) {
				System.out.println(pathConvert);
				file = new File(pathConvert);
				try {
					ConvertExcelToTxt.convertExcelToTxt(path, pathConvert, ",");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println(path);
				file = new File(path);
			}
			/////
			// 7. Kiểm tra file tồn tại
			if (!file.exists()) {
				// 8.1. Cập nhật trạng thái trong data_2file_logs là ERROR at Staging và ngày
				// giờ
				// cập nhật
				System.out.println(pathConvert + " không tồn tại");
				updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "ERROR at Staging", -1);
			} else {
				// 8.2. Đọc file sinh viên và lưu vào một chuỗi string dưới dạng values là sql
				String sql = "INSERT INTO " + lst.get(i).get("table_staging") + " VALUES"
						+ loadStudentFromFile(file, lst.get(i));
				// 9. Tiến hành load tất cả sinh viên vào DB staging từ câu sql và trả về số
				// dòng đã được load thành công
				int count = addStudentOnTable(sql);
				if (count > 0) {
					// 10.1. Cập nhật trạng thái trong data_file_logs là ERROR at Staging và ngày
					// giờ cập nhật, số dòng load thành công = 0
					updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "Staging ok", count);
					System.out.println("Thanh Cong:\t" + "file name: " + lst.get(i).get("filename")
							+ " ==> So dong thanh cong: " + count);
				} else {
					// 10.2. Cập nhật trạng thái trong data_file_logs là ERROR at Staging và ngày
					// giờ cập nhật, số dòng load thành công
					updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "Error at Staging", count);
					System.out.println("ERROR:\t" + "file name: " + lst.get(i).get("filename")
							+ " ==> So dong thanh cong: " + count);
				}

			}

		}
	}

//update trang thai len data file logs
	public static void updateStatusToDataFileLogs(int id, String status, int count) {
		Connection conn = null;
		PreparedStatement pre_control = null;
		String sql = "";
		if (count == -1) {
			sql = "UPDATE data_file_logs SET " + "status_file='" + status
					+ "', data_file_logs.time_staging=NOW() WHERE id=" + id;
		} else {
			sql = "UPDATE data_file_logs SET staging_load_count=" + count + ", status_file='" + status
					+ "', data_file_logs.time_staging=NOW()  WHERE id=" + id;
		} // sua GETDATE() = now() neu dung mySQL
		try {
			conn = new GetConnection().getConnection("control");
			pre_control = conn.prepareStatement(sql);
			pre_control.executeUpdate();
			pre_control.close();
			conn.close();
			System.out.println("Upload status thanh cong!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// add tat ca sinh vien tu file vao bang
	public static int addStudentOnTable(String sql) {
		// mo database
		int count = 0;
		try {
			Connection conn = new GetConnection().getConnection("staging");
			PreparedStatement pre = conn.prepareStatement(sql);
			count = pre.executeUpdate();
			// dong
			pre.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
		return count;
	}

	// tra ve values danh sach cac sinh vien trong cau sql
	public static String loadStudentFromFile(File file, HashMap<String, String> map) {
		String value = "";
		try {
			// Mở file để đọc dữ liệu lên, có kèm theo encoding
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), map.get("encode")));
			// Đọc bỏ phần header
			reader.readLine();
			// Bắt đầu từ hàng thứ 2, đọc từng hàng dữ liệu đến khi cuối file
			String data = reader.readLine();
			System.out.println(data);
			// lay tung hang len
			while (data != null) {
				// cắt hàng theo delimeter lưu trên data_file_logs
				StringTokenizer st = new StringTokenizer(data, map.get("delimiter"));
				System.out.println("count ST: " + st.countTokens());
				// Lưu hàng sinh viên đó vào chuỗi value
//				if (st.countTokens() == Integer.parseInt(map.get("number_column"))) {
				value += "('" + st.nextToken() + "'";
				while (st.hasMoreTokens()) {
					String stt = st.nextToken();
					if (stt.isEmpty()) {
						value += ",N'null'";
					} else {
						value += ", N'" + stt + "'";
					}
				}
				value += "), ";
//				}
				// lay hang tiep theo len
				data = reader.readLine();
				System.out.println(data);

			}
			value = value.substring(0, value.lastIndexOf(","));
			value += ";";
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}

	public static void main(String[] args) {
		//demo test
		new Staging().loadStudentToStaging();
	}
}
