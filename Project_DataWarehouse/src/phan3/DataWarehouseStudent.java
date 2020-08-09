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
public class DataWarehouseStudent {

	public static void main(String[] args) {
		new DataWarehouseStudent().insertStudentToDW();

	}

//	public void insertStudentToDW(String condition) {
	public void insertStudentToDW() {
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
			// 2. Lấy các file sinhvien có trạng thái là OK Staging tại các nhóm có đang active
//			pre_control = con_Control.prepareStatement(
//					"select data_file_logs.id ,data_file_logs.id_config, "
//					+ "data_file_configuaration.data_warehouse_sql,"
//							+ " data_file_configuaration.insert_DW_sql,"
//							+ " data_file_logs.table_staging"
//							+ " from data_file_logs JOIN data_file_configuaration "
//							+ "on data_file_logs.id_config=data_file_configuaration.id "
//							+ "where data_file_configuaration.isActive=1 and data_file_configuaration.id =1 "
//							+ "AND " + condition);
			pre_control = con_Control.prepareStatement("select data_file_logs.id ,data_file_logs.id_config, "
					+ "data_file_configuaration.data_warehouse_sql," + " data_file_configuaration.fileName"
					+ " from data_file_logs JOIN data_file_configuaration "
					+ "on data_file_logs.id_config=data_file_configuaration.id "
					+ "where data_file_configuaration.isActive=1 and data_file_configuaration.id =1 and data_file_logs.status_file like 'Staging ok'");
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();

			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?
				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				// data_warehouse_sql: STT, MSSV,  HoLot, Ten,  NgaySinh,  MaLop, TenLop, SDT, Email, QueQuan, GhiChu
				String sql = re.getString("data_warehouse_sql");// select ***
				String table_src = re.getString("fileName");// from + table staging
				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy STT, MSSV,  HoLot, Ten,  NgaySinh,  MaLop, TenLop, SDT, Email, QueQuan, GhiChu
				//trong table của database Staging
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
					String mssv = re_staging.getString("MSSV");
					String hoLot = re_staging.getString("HoLot");
					String ten = re_staging.getString("Ten");
					String ngaySinh = re_staging.getString("NgaySinh");
					String maLop = re_staging.getString("MaLop");
					String tenLop = re_staging.getString("TenLop");
					String sdt = re_staging.getString("SDT");
					String email = re_staging.getString("Email");
					String queQuan = re_staging.getString("QueQuan");
					String ghiChu = re_staging.getString("GhiChu");

					// chuyen chuoi thanh ngay
					SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
					java.sql.Date sqlDate = null;
					try {
						java.util.Date date = formatter.parse(ngaySinh);
						sqlDate = new java.sql.Date(date.getTime());
						// 8. Mở connection database data_warehouse
						conn_DW1 = new GetConnection().getConnection("warehouse");
						int index_date = getDate(conn_DW1, sqlDate);
						// 9. Truy xuất các field của sinh viên có mã SV là mssv
						// tại các sinh viên đang active
						String sql_exceute = "select * from sinhvien where mssv = '" + mssv + "'";
						pre_DW = conn_DW1.prepareStatement(sql_exceute);
						// 10. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
						ResultSet re_DW = pre_DW.executeQuery();
						int sk_DW = 0;
						String checkExist = "NO";

						String mssvTemp = null;
						String hoLotTemp = null;
						String tenTemp = null;
						String ngaySinhTemp = null;
						String maLopTemp = null;
						String tenLopTemp = null;
						String sdtTemp = null;
						String emailTemp = null;
						String queQuanTemp = null;
						String ghiChuTemp = null;

						while (re_DW.next()) { // Record?
							// Yes: Nhánh 11.2
							mssvTemp = re_DW.getString("MSSV");
							hoLotTemp = re_DW.getString("HoLot");
							tenTemp = re_DW.getString("Ten");
							ngaySinhTemp = re_DW.getString("NgaySinh");
							maLopTemp = re_DW.getString("MaLop");
							tenLopTemp = re_DW.getString("TenLop");
							sdtTemp = re_DW.getString("SDT");
							emailTemp = re_DW.getString("Email");
							queQuanTemp = re_DW.getString("QueQuan");
							ghiChuTemp = re_DW.getString("GhiChu");

							// 11.2.1 So sách các trường còn lại của SV Staging có gì khác không so với
							// SV trong DataWarehouse không?
							if (mssvTemp.equalsIgnoreCase(mssv) && hoLotTemp.equalsIgnoreCase(hoLot)
									&& tenTemp.equalsIgnoreCase(ten) && ngaySinhTemp.equals(ngaySinh)
									&& maLopTemp.equalsIgnoreCase(maLop) && tenLopTemp.equalsIgnoreCase(tenLop)
									&& sdtTemp.equalsIgnoreCase(sdt) && emailTemp.equalsIgnoreCase(email)
									&& queQuanTemp.equalsIgnoreCase(queQuan) && ghiChuTemp.equalsIgnoreCase(ghiChu)) {
								checkExist = "NOCHANGE";// 2 dong y het nhau
								System.out.println("khongdoi");
							} else {
								// 11.2.2.1. Lấy Sk_SV của sinh viên đó
								sk_DW = re_DW.getInt("Sk_SV");
								checkExist = "YES";// co 1 truong nao do khac

							}

						} // end while

						if (checkExist.equalsIgnoreCase("YES")) {
							// *** YES: Tồn tại + có thay đổi: Nhánh 11.2.2

							// 11.2.2.2. In thôn báo thay đổi thông tin SV có mã của nhóm
							System.out.println("==> Thay đôi TTSV mã: " + mssvTemp + ", " + hoLotTemp + " " + tenTemp
									+ ", " + ngaySinhTemp + ", " + maLopTemp + ", " + tenLopTemp + ", " + sdtTemp + ", "
									+ emailTemp + ", " + queQuanTemp + ", " + ghiChuTemp + " thanh " + mssv + ", "
									+ hoLot + " " + ten + ", " + ngaySinh + ", " + maLop + ", " + tenLop + ", " + sdt
									+ ", " + email + ", " + queQuan + ", " + ghiChu);
							// 11.2.2.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
							// ngày giờ hiện tại
							value_update += sk_DW + ", ";
							// 11.2.2.4.Thêm dòng SV vào table Student của data_warehouse
							pre_DW = conn_DW1.prepareStatement(

									"insert into sinhvien( STT , MSSV, HoLot, Ten, NgaySinh ,index_ngaysinh, MaLop, TenLop, SDT, Email, QueQuan, GhiChu) values "
											+ "( '" + stt + "', '" + mssv + "', N'" + hoLot + "', N'" + ten + "', '"
											+ ngaySinh + "', '" + index_date + "', '" + maLop + "', '" + tenLop + "', '"
											+ sdt + "', '" + email + "', '" + queQuan + "', '" + ghiChu + "')");

//
							pre_DW.executeUpdate();

							// 11.2.2.5. tăng số dòng cập nhật lên
							count_UPDATE++;

						} else if (checkExist.equalsIgnoreCase("NO")) {
							// **** NO: them moi hoan toan: Nhanh 11.1

							// 11.1.1. In thông bao thêm SV
							System.out.println("==> them moi SV: " + stt + ", " + mssv + "," + hoLot + "  " + ten + ", "
									+ sqlDate + ", " + maLop + ", " + tenLop + ", " + sdt + ", " + email + ", "
									+ queQuan + ", " + ghiChu + ", ");
							// 11.1.2. Thêm thông tin SV chuỗi value_insert
							value_sql += "(' " + stt + "', '" + mssv + "', N'" + hoLot + "', N'" + ten + "', '"
									+ ngaySinh + "', '" + index_date + "', '" + maLop + "', '" + tenLop + "', '" + sdt
									+ "', '" + email + "', '" + queQuan + "', '" + ghiChu + "'),";
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
								"update sinhvien set isActive = 0 where Sk_SV IN ( " + value_update + ");");
						int update = pre_DW.executeUpdate();
						System.out.println("so dong da update: " + update);
					}
					if (count_NEW > 0) {
						// **them du lieu vao DW

						value_sql = value_sql.substring(0, value_sql.lastIndexOf(","));// cat dau phay cuoi cung
						value_sql += ";";
						System.out.println(value_sql);
						pre_DW = conn_DW1.prepareStatement(
								"insert into sinhvien (STT, MSSV, HoLot, Ten, NgaySinh ,index_ngaysinh, MaLop, TenLop, SDT, Email, QueQuan, GhiChu) values "
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
//			pre = conn_DW.prepareStatement("select Date_SK from Date_dim where Full_date like ?");
//			select sk_date from database_warehouse.date_dim where Full_date =  CONVERT('1999-01-01', DATE);
			pre = conn_DW.prepareStatement("select sk_date from date_dim where Full_date =  CONVERT(?, DATE)");
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
}
