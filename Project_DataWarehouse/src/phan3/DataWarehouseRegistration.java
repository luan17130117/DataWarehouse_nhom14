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
public class DataWarehouseRegistration {
	public static void main(String[] args) {
//		new DataWarehouseRegistration().insertRegistrationToDW("data_file_logs.status_file like 'Ok Staging'");
		new DataWarehouseRegistration().insertRegistrationToDW();
	}

//	public void insertRegistrationToDW(String condition) {
	public void insertRegistrationToDW() {
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
			// 2. Lấy các file dangky có trạng thái là OK Staging tại các nhóm có đang
			// active
			pre_control = con_Control.prepareStatement("select data_file_logs.id ,data_file_logs.id_config, "
					+ "data_file_configuaration.data_warehouse_sql," + " data_file_logs.table_staging"
					+ " from data_file_logs JOIN data_file_configuaration "
					+ "on data_file_logs.id_config=data_file_configuaration.id "
					+ "where data_file_configuaration.isActive=1 and data_file_configuaration.id =4");
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();

			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?
				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				// data_warehouse_sql:STT, MaDK, MSSV, MaLopHoc, TGDK
				String sql = re.getString("data_warehouse_sql");// select ***
				String table_src = re.getString("table_staging");// from + table staging
				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy STT, MaDK, MSSV, MaLopHoc, TGDK trong table của database Staging
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
					String stt = re_staging.getString("STT");
					String maDK = re_staging.getString("MaDK");
					String mssv = re_staging.getString("MSSV");
					String maLopHoc = re_staging.getString("MaLopHoc");
					String tgdk = re_staging.getString("TGDK");

					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {
						java.util.Date date = formatter.parse(tgdk);
						sqlDate = new java.sql.Date(date.getTime());

						// 8. Mở connection database data_warehouse
						conn_DW1 = new GetConnection().getConnection("warehouse");
						int index_dk = getDate(conn_DW1, sqlDate);
						int index_lophoc = getSK_LH(conn_DW1, maLopHoc);
						int index_sinhvien= getSK_SV(conn_DW1, mssv);
						
						// 9. Truy xuất các field của Registation có mã đăng ký là maDK
						String sql_exceute = "select * from registration where MaDK = '" + maDK + "'";
						pre_DW = conn_DW1.prepareStatement(sql_exceute);
						// 10. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
						ResultSet re_DW = pre_DW.executeQuery();
						
						int sk_DW = 0;
						String checkExist = "NO";

						String maDKTemp = null;
						String mssvTemp = null;
						String maLopHocTemp = null;
						String tgdkTemp = null;

						while (re_DW.next()) { // Record?
							// Yes: Nhánh 11.2
							maDKTemp = re_DW.getString("MaDK");
							mssvTemp = re_DW.getString("MSSV");
							maLopHocTemp = re_DW.getString("MaLopHoc");
							tgdkTemp = re_DW.getString("TGDK");

							// 11.2.1 So sách các trường còn lại của dangky Staging có gì khác không so với
							// Registation trong DataWarehouse không?
							if (maDKTemp.equalsIgnoreCase(maDK) && mssvTemp.equalsIgnoreCase(mssv)
									&& maLopHocTemp.equalsIgnoreCase(maLopHoc) && tgdkTemp.equals(tgdk)) {
								checkExist = "NOCHANGE";// 2 dong y het nhau
								System.out.println("khongdoi");
							} else {
								// 11.2.2.1. Lấy Sk_DK của đăng ký đó
								sk_DW = re_DW.getInt("Sk_DK");
								checkExist = "YES";// co 1 truong nao do khac
							}
						} // end while

						if (checkExist.equalsIgnoreCase("YES")) {
							// *** YES: Tồn tại + có thay đổi: Nhánh 11.2.2

							// 11.2.2.2. In thông báo thay đổi thông tin DK
							System.out.println(
									"==> Thay đôi TTDK mã: " + maDKTemp + ", mssv " + mssvTemp + ", ma lop hoc " + maLopHocTemp + ", tgdk "
											+ tgdkTemp + " thanh " + maDK + ", mssv " + mssv + ", ma lop hoc " + maLopHoc + ", tgdk " + tgdk);
							// 11.2.2.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
							// ngày giờ hiện tại
							value_update += sk_DW + ", ";
							// 11.2.2.4.Thêm dòng DK vào table Registration của data_warehouse
							pre_DW = conn_DW1.prepareStatement(
									"insert into registration(STT, MaDK, MSSV, Sk_SV, MaLopHoc, Sk_LH, TGDK, index_dangky) values "
											+ "( '" + stt + "', '" + maDK + "','" + mssv + "','" + index_sinhvien + "','" 
											+ maLopHoc + "', '" + index_lophoc + "', '" + tgdk + "', '" + index_dk + "')");
							//
							pre_DW.executeUpdate();

							// 11.2.2.5. tăng số dòng cập nhật lên
							count_UPDATE++;

						} else if (checkExist.equalsIgnoreCase("NO")) {
							// **** NO: them moi hoan toan: Nhanh 11.1

							// 11.1.1. In thông bao thêm DK
							System.out.println("==> them moi DK: stt " + stt + ", ma dang ky " + maDK + ", mssv " + mssv + ", ma lop hoc " 
							+ maLopHoc+ ", ma dang ky " + sqlDate + ", ");
							// 11.1.2. Thêm thông tin DK chuỗi value_insert
							value_sql += "( '" + stt + "', '" + maDK + "','" + mssv + "','" + index_sinhvien + "','" 
							+ maLopHoc + "', '" + index_lophoc + "', '" + tgdk + "', '" + index_dk + "'),";
							// 11.1..3. tăng số dòng thêm mới lên
							count_NEW++;

						} else if (checkExist.equalsIgnoreCase("NOCHANGE")) {
							// *** NOCHANGE: giong y chang, khong co gi thay doi: Nhanh 11.2.1

							System.out.println("==> KHONG CO GI THAY DOI: TT trong DW");
							// 11.2.1.1. Tăng số dòng không cần thêm vào data_warehouse lên 1
							countEXIST++;
						}
					} catch (ParseException e) {
						e.printStackTrace();
						err = true;
						System.out.println("ngay khong dung dinh dang");
					}

				} // end while:1 DK trong staging

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
								"update registration set isActive = 0 where Sk_DK IN ( " + value_update + ");");
						int update = pre_DW.executeUpdate();
						System.out.println("so dong da update: " + update);
					}
					if (count_NEW > 0) {
						// **them du lieu vao DW

						value_sql = value_sql.substring(0, value_sql.lastIndexOf(","));// cat dau phay cuoi cung
						value_sql += ";";
						System.out.println(value_sql);
						pre_DW = conn_DW1.prepareStatement(
								"insert into registration (STT, MaDK, MSSV, Sk_SV, MaLopHoc, Sk_LH, TGDK, index_dangky) values "
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
			// 11.1.1. Tìm Date_SK của ngày đăng ký trong Date_dim table
//				pre = conn_DW.prepareStatement("select Date_SK from Date_dim where Full_date like ?");
//				select sk_date from database_warehouse.date_dim where Full_date =  CONVERT('1999-01-01', DATE);
			pre = conn_DW.prepareStatement("select sk_date from date_dim where Full_date like ?");
			pre.setDate(1, dob);
			ResultSet re_date = pre.executeQuery();
			int sk = 0;
			if (re_date.next()) {
				sk = re_date.getInt("sk_date");
			}
			return sk;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int getSK_LH(Connection conn_DW, String maLH) {
		PreparedStatement pre = null;
		try {
			pre = conn_DW.prepareStatement("select Sk_LH from class where MaLopHoc like ? and isActive = 1");
			pre.setString(1, maLH);
			ResultSet re = pre.executeQuery();
			int sk_LH = 0;
			if (re.next()) {
				sk_LH = re.getInt("Sk_LH");
			}
			return sk_LH;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;

	}

	public int getSK_SV(Connection conn_DW, String mssv) {
		PreparedStatement pre = null;
		try {
			pre = conn_DW.prepareStatement("select Sk_SV from student where MSSV like ? and isActive = 1");
			pre.setString(1, mssv);
			ResultSet re = pre.executeQuery();
			int sk_SV = 0;
			if (re.next()) {
				sk_SV = re.getInt("Sk_SV");
			}
			return sk_SV;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}
