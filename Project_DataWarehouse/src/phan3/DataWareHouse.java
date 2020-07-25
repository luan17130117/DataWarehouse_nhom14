package phan3;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import connection.database.GetConnection;

//Chuyen Staging vao Data_Warehouse
public class DataWareHouse {

	public static void main(String[] args) {
		new DataWareHouse().insertFromStagingToDW_OPTIMIZE("data_file_logs.status_file like 'Ok Staging'");
	}

	public void insertFromStagingToDW_OPTIMIZE(String condition) {
		// ket noi voi Control
		Connection con_Control = null;
		PreparedStatement pre_control = null;
		// ket noi cho Staging
		Connection conn_staging = null;
		PreparedStatement pre_staging = null;
		// mo cho Datawarehouse
		Connection conn_DW1 = null;
		PreparedStatement pre_DW = null;

		String value_sql = null;
		String value_update = null;
		try {
			// 1. Kết nối DB control
			con_Control = new GetConnection().getConnection("control");
			// 2. Lấy các file có trạng thái là OK Staging tại các nhóm có đang active
			pre_control = con_Control.prepareStatement(
					"select data_file_logs.id ,data_file_logs.ID_host, data_file_configuaration.data_warehouse_sql,"
							+ " data_file_configuaration.insert_DW_sql, data_file_configuaration.table_staging_load"
							+ " from data_file_logs JOIN data_file_configuaration "
							+ "on data_file_logs.ID_host=data_file_configuaration.id "
							+ "where data_file_configuaration.isActive=1 AND " + condition);
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?

				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				int maGroup = re.getInt("ID_host");// ma group
				// data_warehouse_sql: mssv, cast([ho] as varchar(100)) + ' ' + cast([ten] as
				// varchar(100))as hoten,
				// ngaysinh, gioitinh
				String sql = re.getString("data_warehouse_sql");// select ***
				String table_src = re.getString("table_staging_load");// from + table staging

				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy mssv, hoten, ngay sinh, gioitinh trong table của database Staging
				pre_staging = conn_staging.prepareStatement("select " + sql + " from " + table_src);
				// 7. Trả về Result Set chứa các record thỏa điều kiện
				ResultSet re_staging = pre_staging.executeQuery();
				boolean err = false;
				value_sql = "";
				value_update = "";

				if (re_staging.isBeforeFirst() == false) {
					System.out.println("KHONG CÓ DATA TRONG STAGING");
					System.exit(0);
				}

				while (re_staging.next()) {// chay tung Record?
					String id = re_staging.getString("mssv");
					String full_name = re_staging.getString("hoten");
					String ngay = re_staging.getString("ngaysinh");
					
					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {
						java.util.Date date = formatter.parse(ngay);
						sqlDate = new java.sql.Date(date.getTime());

						// 8. Mở connection database data_warehouse
						conn_DW1 = new GetConnection().getConnection("warehouse");
						int index_date = getDate(conn_DW1, sqlDate);
						// 9. Truy xuất SK của sinh viên có mã SV là id và các file của nhóm là maGroup
						// tại các sinh viên đang active
						String sql_exceute = "select * from Student where id = '" + id + "' and id_file_group = "
								+ maGroup + " and isActive = 1";
						pre_DW = conn_DW1.prepareStatement(sql_exceute);
						// 10. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
						ResultSet re_DW = pre_DW.executeQuery();
						int sk_DW = 0;
						String checkExist = "NO";
						String nameTemp = null;
						Date dob_Temp = null;
						String gender_Temp = null;
						String id_temp = null;
						
						if (re_DW.next()) { // Record?
							// Yes: Nhánh 11.2
							id_temp = re_DW.getString("id");
							nameTemp = re_DW.getString("full_name");
							dob_Temp = re_DW.getDate("dob");
							gender_Temp = re_DW.getString("gender");
							// 11.2.1 So sách các trường còn lại của SV Staging có gì khác không so với
							// SV trong DataWarehouse không?
							if (nameTemp.equalsIgnoreCase(full_name) && dob_Temp.equals(sqlDate)) {
								checkExist = "NOCHANGE";// 2 dong y het nhau
							} else {
								// 11.2.2.1. Lấy sk của sinh viên đó
								sk_DW = re_DW.getInt("sk");
								checkExist = "YES";// co 1 truong nao do khac
							}

						} // end while

						if (checkExist.equalsIgnoreCase("YES")) {
							// *** YES: Tồn tại + có thay đổi: Nhánh 11.2.2

							// 11.2.2.2. In thôn báo thay đổi thông tin SV có mã của nhóm
							System.out.println("==> Thay đôi TTSV mã: " + id_temp + ", " + nameTemp + ", " + gender_Temp
									+ ", " + dob_Temp + " , cua nhom " + maGroup + " THANH " + id + ", " + full_name
									+ ", "  + sqlDate);
							// 11.2.2.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
							// ngày giờ hiện tại
							value_update += sk_DW + ", ";

							// 11.2.2.4.Thêm dòng SV vào table Student của data_warehouse
							pre_DW = conn_DW1.prepareStatement(
									"insert into Student(id, full_name, dob, index_ngaysinh, gender, file_src, id_file_group) values "
											+ "( '" + id + "', N'" + full_name + "', '" + sqlDate + "', " + index_date
											+ "', '" + table_src + "', " + maGroup + ")");
							int i = pre_DW.executeUpdate();

							// value_sql += "( '" + id + "', N'" + full_name + "', '" + sqlDate + "', " +
							// index_date
							// + ", N'" + gender + "', '" + table_src + "', " + maGroup + "),";
							// 11.2.2.5. tăng số dòng cập nhật lên
							count_UPDATE++;

						} else if (checkExist.equalsIgnoreCase("NO")) {
							// **** NO: them moi hoan toan: Nhanh 11.1

							// 11.1.1. In thông bao thêm SV
							System.out.println("==> them moi SV: " + id + ", " + full_name +  ", "
									+ sqlDate + ", cua nhom " + maGroup);
							// 11.1.2. Thêm thông tin SV chuỗi value_insert
							value_sql += "( '" + id + "', N'" + full_name + "', '" + sqlDate + "', " + index_date
									+  "', '" + table_src + "', " + maGroup + "),";
							// 11.1..3. tăng số dòng thêm mới lên
							count_NEW++;

						} else if (checkExist.equalsIgnoreCase("NOCHANGE")) {
							// *** NOCHANGE: giong y chang, khong co gi thay doi: Nhanh 11.2.1

							System.out.println("==> KHONG CO GI THAY DOI: TT trong DW" + id_temp + ", " + nameTemp
									+ ", " + gender_Temp + ", " + dob_Temp + " , cua nhom " + maGroup
									+ " TT trong Staging " + id + ", " + full_name + ", " + sqlDate);
							// 11.2.1.1. Tăng số dòng không cần thêm vào data_warehouse lên 1
							countEXIST++;
						}
					} catch (ParseException e) {
						e.printStackTrace();
						err = true;
						System.out.println("ngay khong dung dinh dang");
					}

				} // end while:1 SV trong staging

				// kiem tra ERR eps kieu cho ngay thoi
				if (err == true) {
					// 12.b. Update trạng thái file là ERROR_DATE_DW và time_data_warehouse là TG
					// hiện tại
					pre_control = con_Control.prepareStatement("update data_file_logs set status_file='ERROR DW' ,"
							+ "data_file_logs.time_data_warehouse=now() where id=" + id_file);
					pre_control.executeUpdate();
					System.out.println("update error!! " + id_file);
				} else {
					// ****** het table trong staging
					// ******* cap nhat vao DW
					if (count_UPDATE > 0) {

						value_update = value_update.substring(0, value_update.lastIndexOf(","));// cat dau , cuoi cung
						pre_DW = conn_DW1.prepareStatement(
								"update Student set isActive = 0, date_change=getDate() where sk IN ( " + value_update
										+ ");");
						int update = pre_DW.executeUpdate();
						System.out.println("so dong da update: " + update);
					}
					if (count_NEW > 0) {
						// **them du lieu vao DW
						value_sql = value_sql.substring(0, value_sql.lastIndexOf(","));// cat dua phay cuoi cung
						value_sql += ";";
						pre_DW = conn_DW1.prepareStatement(
								"insert into Student(id, full_name, dob, index_ngaysinh, gender, file_src, id_file_group) values "
										+ value_sql);
						int i = pre_DW.executeUpdate();
						System.out.println("So dong insert vao: " + i);
					}

					// 12.a. Update trạng thái file là OK DW và time_data_warehouse là TG hiện
					// tại
					pre_control = con_Control.prepareStatement(
							"update data_file_logs set status_file='OK DW' , data_file_logs.time_data_warehouse=now(), exist_row_DW ="
									+ countEXIST + " , row_new_DW=" + count_NEW + ", row_update_DW = " + count_UPDATE
									+ "   where id=" + id_file);
					pre_control.executeUpdate();
					System.out.println("update staus_file = OK DW " + id_file);
				}

				// 13 truncate table
				pre_staging = conn_staging.prepareStatement("truncate table " + table_src);
				pre_staging.executeUpdate();
				System.out.println("truncate done!! " + table_src);

				value_sql = null;
				// close
				pre_DW.close();
				conn_DW1.close();
				conn_staging.close();
			} // end while control

			// 14. Đóng tất cả kết nối
			pre_control.close();
			con_Control.close();

		} catch (SQLException e1) {
			// loi ket noi toi DB
			// In ra man hình lỗi kết nối
			e1.printStackTrace();
			System.out.println(e1.getMessage());
		}
	}

	// chen 1 dong vao DW
	public int getDate(Connection conn_DW, Date dob) {

		PreparedStatement pre = null;
		try {
			// 11.1.1. Tìm Date_SK của ngày sinh sinh trong Date_dim table
			pre = conn_DW.prepareStatement("select Date_SK from Date_dim where Full_date like ?");
			pre.setDate(1, dob);
			ResultSet re_date = pre.executeQuery();
			int sk = 0;
			if (re_date.next()) {
				sk = re_date.getInt("Date_SK");
			}
			return sk;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	// chen 1 dong vao DW
	public boolean insertALine(Connection conn_DW, String sql, String id, String full_name, String gender, Date dob,
			String table_Staging, int idHostGroup) {

		int i = 0;
		PreparedStatement pre = null;
		try {
			// 11.1.1. Tìm Date_SK của ngày sinh sinh trong Date_dim table
			pre = conn_DW.prepareStatement("select Date_SK from Date_dim where Full_date like ?");
			pre.setDate(1, dob);
			ResultSet re_date = pre.executeQuery();
			int sk = 0;
			while (re_date.next()) {
				sk = re_date.getInt("Date_SK");
			}

			// 11.1.2. Thêm thông tin cần thiết của SV đó vào Student table của database
			// data_warehouse
			pre = conn_DW.prepareStatement("insert into " + sql);
			pre.setString(1, id);// ma sv
			pre.setString(2, full_name);// hoten
			pre.setDate(3, dob);// ngay sinh
			pre.setInt(4, sk);// index_ngaysinh
			
			pre.setString(6, table_Staging);// file_source
			pre.setInt(7, idHostGroup);

			i = pre.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (i > 0) {
			return true;
		}
		return false;
	}


	
	
	
}
