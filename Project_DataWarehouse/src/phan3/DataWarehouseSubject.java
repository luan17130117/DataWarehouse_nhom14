package phan3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connection.database.GetConnection;

//Chuyen Staging vao Data_Warehouse
public class DataWarehouseSubject {
	public static void main(String[] args) {
		new DataWarehouseSubject().insertSubjectToDW();
	}

	public void insertSubjectToDW() {
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
			// 2. Lấy các file monhoc có trạng thái là Staging ok tại các nhóm có đang
			// active
			pre_control = con_Control.prepareStatement("select data_file_logs.id ,data_file_logs.id_config, "
					+ "data_file_configuaration.data_warehouse_sql," + " data_file_configuaration.fileName"
					+ " from data_file_logs JOIN data_file_configuaration "
					+ "on data_file_logs.id_config=data_file_configuaration.id "
					+ "where data_file_configuaration.isActive=1 and data_file_configuaration.id =2 "
					+ "and data_file_logs.status_file like 'Staging ok'");

			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?
				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				// data_warehouse_sql:STT, MaMH, TenMonHoc, TC, KhoaQuanLy, KhoaSuDung
				String sql = re.getString("data_warehouse_sql");// select ***
				String table_src = re.getString("fileName");// from + table staging
				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy STT, MaMH, TenMonHoc, TC, KhoaQuanLy, KhoaSuDung
				// trong table của database Staging
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
					String mmh = re_staging.getString("MaMH");
					String tenMonHoc = re_staging.getString("TenMonHoc");
					String tc = re_staging.getString("TC");
					String khoaQuanLy = re_staging.getString("KhoaQuanLy");
					String khoaSD = re_staging.getString("KhoaSuDung");

					// 8. Mở connection database data_warehouse
					conn_DW1 = new GetConnection().getConnection("warehouse");
					// 9. Truy xuất các field của monhoc trong data_warehouse có mã MH là mmh
					// tại các monhoc đang active
					String sql_exceute = "select * from monhoc where MaMH = '" + mmh + "'";
					pre_DW = conn_DW1.prepareStatement(sql_exceute);
					// 10. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
					ResultSet re_DW = pre_DW.executeQuery();
					int sk_DW = 0;
					String checkExist = "NO";

					String mmhTemp = null;
					String tenMonHocTemp = null;
					String tcTemp = null;
					String khoaQuanLyTemp = null;
					String khoaSDTemp = null;

					while (re_DW.next()) { // Record?
						// Yes: Nhánh 11.2
						mmhTemp = re_DW.getString("MaMH");
						System.out.println(mmhTemp);
						tenMonHocTemp = re_DW.getString("TenMonHoc");
						System.out.println(tenMonHocTemp);
						tcTemp = re_DW.getString("TC");
						System.out.println(tcTemp);
						khoaQuanLyTemp = re_DW.getString("KhoaQuanLy");
						System.out.println(khoaQuanLyTemp);
						khoaSDTemp = re_DW.getString("KhoaSuDung");
						System.out.println(khoaSDTemp);

						// 11.2.1 So sách các trường còn lại của MH Staging có gì khác không so với
						// trong DataWarehouse không?
						if (mmhTemp.equalsIgnoreCase(mmh) && tenMonHocTemp.equalsIgnoreCase(tenMonHoc)
								&& tcTemp.equalsIgnoreCase(tc) && khoaQuanLyTemp.equals(khoaQuanLy)
								&& khoaSDTemp.equalsIgnoreCase(khoaSD)) {
							checkExist = "NOCHANGE";// 2 dong y het nhau
							System.out.println("khongdoi");
						} else {
							// 11.4.1. Lấy Sk_MH của môn học đó
							sk_DW = re_DW.getInt("Sk_MH");
							checkExist = "YES";// co 1 truong nao do khac
						}

					} // end while

					if (checkExist.equalsIgnoreCase("YES")) {
						// *** YES: Tồn tại + có thay đổi: Nhánh 11.4
						
						// 11.4.2. In thông báo thay đổi thông tin MH
						System.out.println("==> Thay đôi TTMH mã: " + mmhTemp + ", ten mon hoc " + tenMonHocTemp
								+ ", tc " + tcTemp + ", khoa quan ly " + khoaQuanLyTemp + ", khoa sd " + khoaSDTemp
								+ " thanh " + mmh + ", ten mon hoc " + tenMonHoc + ", tc " + tc + ", khoa quan ly "
								+ khoaQuanLyTemp + ", khoa sd " + khoaSD);
						// 11.4.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
						// ngày giờ hiện tại
						value_update += sk_DW + ", ";
						// 11.4.4.Thêm dòng MH vào table subject của data_warehouse
						pre_DW = conn_DW1.prepareStatement(
								"insert into monhoc ( STT, MaMH, TenMonHoc, TC, KhoaQuanLy, KhoaSuDung) values " + "( '"
										+ stt + "', '" + mmh + "', N'" + tenMonHoc + "','" + tc + "',N'" + khoaQuanLy
										+ "',N'" + khoaSD + "')");

//
						pre_DW.executeUpdate();

						// 11.4.5. tăng số dòng cập nhật lên
						count_UPDATE++;

					} else if (checkExist.equalsIgnoreCase("NO")) {
						// **** NO: them moi hoan toan: Nhanh 11.1

						// 11.1.1. In thông bao thêm MH
						System.out.println("==> them moi MH: stt" + stt + ", ma mon " + mmh + ", ten hoc " + tenMonHoc
								+ ", tc " + tc + ", khoa quan ly " + khoaQuanLy + ", khoa sd " + khoaSD + ", ");

						// 11.1.2. Thêm thông tin MH chuỗi value_insert
						value_sql += "( '" + stt + "', '" + mmh + "', N'" + tenMonHoc + "','" + tc + "', N'"
								+ khoaQuanLy + "',N'" + khoaSD + "'),";
						// 11.1.3. tăng số dòng thêm mới lên
						count_NEW++;

					} else if (checkExist.equalsIgnoreCase("NOCHANGE")) {
						// *** NOCHANGE: giong y chang, khong co gi thay doi: Nhanh 11.3

						System.out.println("==> KHONG CO GI THAY DOI: TT trong DW");
						// 11.3.1. Tăng số dòng không cần thêm vào data_warehouse lên 1
						countEXIST++;
					}

				} // end while:1 MH trong staging

				// kiem tra ERR ep kieu cho ngay
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
//					Nhanh 12.a
					if (count_UPDATE > 0) {
						value_update = value_update.substring(0, value_update.lastIndexOf(","));// cat dau , cuoi cung
						//12.a.1 Trong database data-warehouse Cập nhật isActive = 0
						pre_DW = conn_DW1.prepareStatement(
								"update monhoc set isActive = 0 where Sk_MH IN ( " + value_update + ");");
						int update = pre_DW.executeUpdate();
						System.out.println("so dong da update: " + update);
					}
					if (count_NEW > 0) {
						// 12.a.2 them du lieu vao DW
						value_sql = value_sql.substring(0, value_sql.lastIndexOf(","));// cat dau phay cuoi cung
						value_sql += ";";
						System.out.println(value_sql);
						pre_DW = conn_DW1.prepareStatement(
								"insert into monhoc (STT, MaMH, TenMonHoc, TC, KhoaQuanLy, KhoaSuDung) values "
										+ value_sql);
						int i = pre_DW.executeUpdate();
						System.out.println("So dong insert vao: " + i);
					}

					// 12.a.3 Update trạng thái file là OK DW và time_data_warehouse là TG hiện
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
}
