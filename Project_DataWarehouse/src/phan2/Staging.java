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
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import connection.database.GetConnection;
import phan3.DataWarehouseClass;
import phan3.DataWarehouseRegistration;
import phan3.DataWarehouseStudent;
import phan3.DataWarehouseSubject;

//Step2: Load data file from local to DB Staging
public class Staging {
	// load sinh vien vao db staging theo tung file
	public static void loadStudentToStaging() {
		//1
		ArrayList<HashMap<String, String>> lst = getFileInControl();
		System.out.println(lst.size());
		String db_staging = new GetConnection().getDbName();
		// 6. Duyệt danh sách các file
		try {
		for (int i = 0; i < lst.size(); i++) {
			String fileName = lst.get(i).get("fileName");
			String fileNamei = lst.get(i).get("yourFileName");
			String fileNameo = fileNamei.substring(0, fileNamei.lastIndexOf(".")) + ".txt";
			String path = lst.get(i).get("dir") + File.separator + fileName + File.separator + fileNamei;
			String pathConvert = lst.get(i).get("dir") + File.separator + "convert" + File.separator + fileName
					+ File.separator + fileNameo;
			File file = null;
			////
			if (fileNamei.substring(fileNamei.lastIndexOf(".")).equalsIgnoreCase(".xlsx")
					|| fileNamei.substring(fileNamei.lastIndexOf(".")).equalsIgnoreCase(".xls")) {
				System.out.println(pathConvert);
				file = new File(pathConvert);
				try {
					ConvertExcelToTxt.convertExcelToTxt(path, pathConvert, ";");
				} catch (IOException | NullPointerException e) {
					// TODO Auto-generated catch block
					System.out.println("<---> ERROR [ConvertExcelToTxt]: " + e.getMessage());
				}
				path = pathConvert;
				fileNamei = fileNameo;
			} else if (fileNamei.substring(fileNamei.lastIndexOf(".")).equalsIgnoreCase(".csv")
					|| fileNamei.substring(fileNamei.lastIndexOf(".")).equalsIgnoreCase(".txt")) {
				System.out.println(path);
				file = new File(path);
			} else {
				System.out.println("File không đúng định dạng");
				updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "ERROR at Staging", -1, "");
				continue;
			}
			// 7. Kiểm tra file tồn tại
			if (!file.exists()) {
				// 8.1. Cập nhật trạng thái trong data_2file_logs là ERROR at Staging và ngày
				// giờ
				// cập nhật
				System.out.println(path + " không tồn tại");
				updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "ERROR at Staging", -1, "");
			} else {
				// 8.2. Đọc file sinh viên và lưu vào một chuỗi string dưới dạng values là sql
				String sql = "INSERT INTO " + fileName + " VALUES" + loadStudentFromFile(file, lst.get(i));
				// 9. Tiến hành load tất cả sinh viên vào DB staging từ câu sql và trả về số
				// dòng đã được load thành công
				int count = addStudentOnTable(sql);

				if (count > 0) {
					// 10.1. Cập nhật trạng thái trong data_file_logs là ERROR at Staging và ngày
					// giờ cập nhật, số dòng load thành công = 0
					updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "Staging ok", count,
							lst.get(i).get("fileName"));
					System.out
							.println("Thanh Cong:\t" + "file name: " + fileNamei + " ==> So dong thanh cong: " + count);
				} else {
					// 10.2. Cập nhật trạng thái trong data_file_logs là ERROR at Staging và ngày
					// giờ cập nhật, số dòng load thành công
					updateStatusToDataFileLogs(Integer.parseInt(lst.get(i).get("id")), "Error at Staging", count,
							lst.get(i).get("fileName"));
					System.out.println("ERROR:\t" + "file name: " + fileNamei + " ==> So dong thanh cong: " + count);
				}

			}
			//Phần 3
			int n = Integer.parseInt(lst.get(i).get("id_config"));
			if(n==1) {
				new DataWarehouseStudent().insertStudentToDW();
			}
			if(n==2) {
				new DataWarehouseSubject().insertSubjectToDW();
			}
			if(n==3) {
				new DataWarehouseClass().insertClassToDW();
			}
			if(n==4) {
				new DataWarehouseRegistration().insertRegistrationToDW();
			}

		}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("<---> ERROR [LoadStudentToStaging]: " + e.getMessage());
		}
	}

	// tra ve danh sach cac file da tai ve tu SCP va san sang load vao staging
	public static ArrayList<HashMap<String, String>> getFileInControl() {
		ArrayList<HashMap<String, String>> lst = new ArrayList<>();
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// 1. Kết nối tới DB control
			conn = new GetConnection().getConnection("control");
			// 2. Tìm tất cả các file có trạng thái OK download và ở các nhóm đang active
			pre_control = conn.prepareStatement("SELECT data_file_logs.id ,id_config, your_filename, "
					+ " delimiter, localPath, fileName, number_column "
					+ "from data_file_logs JOIN data_file_configuaration "
					+ "ON data_file_logs.id_config = data_file_configuaration.id" + " where "
					+ "data_file_logs.status_file like 'Download ok' AND data_file_configuaration.isActive=1 ");
			// 3. Nhận được ResultSet chứa các record thỏa điều kiện truy xuất
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng dòng record và lấy các giá trị put vào Map
			while (re.next()) {
				HashMap<String, String> hm = new HashMap<String, String>();
				hm.put("id", Integer.toString(re.getInt("id")));
				hm.put("id_config", Integer.toString(re.getInt("id_config")));
				hm.put("fileName", re.getString("fileName"));
				hm.put("dir", re.getString("localPath"));
				hm.put("yourFileName", re.getString("your_filename"));
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
			System.out.println("<---> ERROR [Get File In Control]: " + e.getMessage());
			return lst;
		}
	}

	// tra ve values danh sach cac sinh vien trong cau sql
	public static String loadStudentFromFile(File file, HashMap<String, String> map) {
		String value = "";
		try {
			// Mở file để đọc dữ liệu lên, có kèm theo encoding
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			// Đọc bỏ phần header
			reader.readLine();
			// Bắt đầu từ hàng thứ 2, đọc từng hàng dữ liệu đến khi cuối file
			String data = reader.readLine();
			// lay tung hang len
			while (data != null) {
				System.out.println(data);
				// cắt hàng theo delimeter lưu trên data_file_logs
				StringTokenizer st = new StringTokenizer(data, map.get("delimiter"));
				System.out.println("count ST: " + st.countTokens());
				// Lưu hàng sinh viên đó vào chuỗi value
				value += "('" + st.nextToken() + "'";
				for (int i = 1; i < Integer.parseInt(map.get("number_column")); i++) {
//						st.hasMoreTokens();
					String stt = "";
					if (st.hasMoreTokens()) {
						stt = st.nextToken();
					}
					if (stt.isEmpty()) {
						value += ",'?'";
					} else {
						value += ", '" + stt + "'";
					}
				}
				value += "), ";
				// lay hang tiep theo len
				data = reader.readLine();
			}
			value = value.substring(0, value.lastIndexOf(","));
			value += ";";
			reader.close();
		} catch (IOException | NoSuchElementException | StringIndexOutOfBoundsException e) {
			System.out.println("<---> ERROR [Load Student From File]: " + e.getMessage());
//				System.out.println("<---> ERROR [Load Student From File]: " + e.toString());
		}
		return value;
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
//				e.printStackTrace();
			System.out.println("<---> ERROR [Add Student On Table]: " + e.getMessage());
			return 0;
		}
		return count;
	}

	// update trang thai len data file logs
	public static void updateStatusToDataFileLogs(int id, String status, int count, String fileName) {
		Connection conn = null;
		PreparedStatement pre_control = null;
		String sql = "";
		if (count == -1) {
			sql = "UPDATE data_file_logs SET " + "status_file='" + status
					+ "', data_file_logs.time_staging=NOW() WHERE id=" + id;
		} else {
			sql = "UPDATE data_file_logs SET staging_load_count=" + count + ", status_file='" + status
					+ "', data_file_logs.time_staging=NOW() WHERE id=" + id;
		} // sua GETDATE() = now() neu dung mySQL
		try {
			conn = new GetConnection().getConnection("control");
			pre_control = conn.prepareStatement(sql);
			pre_control.executeUpdate();
			pre_control.close();
			conn.close();
			System.out.println("Upload status thanh cong!");
		} catch (Exception e) {
//			e.printStackTrace();
			System.out.println("<---> ERROR [Update Status To Data File Logs]: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		new Staging().loadStudentToStaging();
	}
}
