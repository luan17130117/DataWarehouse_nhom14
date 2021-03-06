package phan3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import connection.database.GetConnection;

//Chuyen Staging vao Data_Warehouse
public class DataWarehouseClass {

	public static void main(String[] args) {
		new DataWarehouseClass().insertClassToDW();
	}

	public void insertClassToDW() {
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
			// 2. Lấy các file lophoc có trạng thái là Staging ok và đang active
			pre_control = con_Control.prepareStatement("select data_file_logs.id ,data_file_logs.id_config, "
					+ "data_file_configuaration.data_warehouse_sql," + " data_file_configuaration.fileName"
					+ " from data_file_logs JOIN data_file_configuaration "
					+ "on data_file_logs.id_config=data_file_configuaration.id "
					+ "where data_file_configuaration.isActive=1 and data_file_configuaration.id =3 "
					+ "and data_file_logs.status_file like 'Staging ok'");
			// 3. Trả về Result set chứa các record thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 4. Chạy từng record trong result set ==> tung cai ten tablename trong Staging
			while (re.next()) {// Record?
				int count_NEW = 0;
				int count_UPDATE = 0;
				int countEXIST = 0;

				int id_file = re.getInt("id"); // ma file
				// data_warehouse_sql:STT, MaLopHoc, MaMH, NamHoc
				String sql = re.getString("data_warehouse_sql");// select ***
				String table_src = re.getString("fileName");// from + table staging
				// 5. Mở connection của database Staging
				conn_staging = new GetConnection().getConnection("staging");
				// 6. Lấy STT, MaLopHoc, MaMH, NamHoc
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
					String maLop = re_staging.getString("MaLopHoc");
					String mmh = re_staging.getString("MaMH");
					String namHoc = re_staging.getString("NamHoc");

					// 8. Mở connection database data_warehouse
					conn_DW1 = new GetConnection().getConnection("warehouse");

//					int maMH = Integer.parseInt(mmh); 
					int index_monhoc = getSK_MH(conn_DW1, mmh);

					// 9. Truy xuất các field của lophoc trong data_warehouse có mã lớp là maLop
					// tại các lophoc đang active
					String sql_exceute = "select * from lophoc where MaLopHoc = '" + maLop + "'";
					pre_DW = conn_DW1.prepareStatement(sql_exceute);
					// 10. Trả về ResultSet chứa 1 record thỏa điều kiện truy xuất
					ResultSet re_DW = pre_DW.executeQuery();
					int sk_DW = 0;
					String checkExist = "NO";

					String maLopTemp = null;
					String mmhTemp = null;
					String namHocTemp = null;

					while (re_DW.next()) { // Record?
						// Yes: Nhánh 11.2

						maLopTemp = re_DW.getString("MaLopHoc");
						mmhTemp = re_DW.getString("MaMH");
						namHocTemp = re_DW.getString("NamHoc");

						// 11.2.1 So sách các trường còn lại của LH Staging có gì khác không so với
						// class trong DataWarehouse không?
						if (maLopTemp.equalsIgnoreCase(maLop) && mmhTemp.equalsIgnoreCase(mmh)
								&& namHocTemp.equalsIgnoreCase(namHoc)) {
							checkExist = "NOCHANGE";// 2 dong y het nhau
							System.out.println("khongdoi");
						} else {
							// 11.4.1. Lấy Sk_LH của lớp học đó
							sk_DW = re_DW.getInt("Sk_LH");
							checkExist = "YES";// co 1 truong nao do khac

						}

					} // end while

					if (checkExist.equalsIgnoreCase("YES")) {
						// *** YES: Tồn tại + có thay đổi: Nhánh 11.4
						
						// 11.4.2. In thôn báo thay đổi thông tin LH
						System.out.println("==> Thay doi TTLH: ma lop " + maLopTemp + ", ma mon hoc " + mmhTemp
								+ ", nam hoc " + namHocTemp + " thanh ma lop " + maLop + ", ma mon hoc " + mmh
								+ ", nam hoc " + namHoc);
						// 11.4.3. Trong database data-warehouse Cập nhật isActive = 0, date_change là
						// ngày giờ hiện tại
						value_update += sk_DW + ", ";
						// 11.4.4.Thêm dòng LH vào table lophoc của data_warehouse
						pre_DW = conn_DW1.prepareStatement(
								"insert into lophoc (STT, MaLopHoc, MaMH, Sk_MH, NamHoc) values " + "( '" + stt + "', '"
										+ maLop + "','" + mmh + "','" + index_monhoc + "','" + namHoc + "')");
						pre_DW.executeUpdate();

						// 11.4.5. tăng số dòng cập nhật lên
						count_UPDATE++;

					} else if (checkExist.equalsIgnoreCase("NO")) {
						// **** NO: them moi hoan toan: Nhanh 11.1

						// 11.1.1. In thông bao thêm LH
						System.out.println("==> them moi LH: STT " + stt + ", ma lop " + maLop + ", ma mon hoc " + mmh
								+ ", nam hoc " + namHoc + ", ");

						// 11.1.2. Thêm thông tin LH chuỗi value_insert
						value_sql += "( '" + stt + "', '" + maLop + "','" + mmh + "','" + index_monhoc + "','" + namHoc
								+ "'),";
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
//					Nhanh 12.a
					// ****** het table trong staging
					// ******* cap nhat vao DW
					if (count_UPDATE > 0) {
						value_update = value_update.substring(0, value_update.lastIndexOf(","));// cat dau , cuoi cung
						//12.a.1 Trong database data-warehouse Cập nhật isActive = 0
						pre_DW = conn_DW1.prepareStatement(
								"update lophoc set isActive = 0 where Sk_LH IN ( " + value_update + ");");
						int update = pre_DW.executeUpdate();
						System.out.println("so dong da update: " + update);
					}
					if (count_NEW > 0) {
						// 12.a.2 them du lieu vao DW
						value_sql = value_sql.substring(0, value_sql.lastIndexOf(","));// cat dau phay cuoi cung
						value_sql += ";";
						System.out.println(value_sql);
						pre_DW = conn_DW1.prepareStatement(
								"insert into lophoc (STT, MaLopHoc, MaMH, Sk_MH, NamHoc) values " + value_sql);
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

	
	public int getSK_MH(Connection conn_DW, String maMH) {
		PreparedStatement pre = null;
		try {
			//tim Sk_MH cua MaMH trong bang monhoc
			pre = conn_DW.prepareStatement("select Sk_MH from monhoc where MaMH like ? and isActive = 1");
			pre.setString(1, maMH);
			ResultSet re = pre.executeQuery();
			int sk_MH = 0;
			if (re.next()) {
				sk_MH = re.getInt("Sk_MH");
			}
			return sk_MH;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;

	}

}
