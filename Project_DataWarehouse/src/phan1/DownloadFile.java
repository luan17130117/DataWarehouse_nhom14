package phan1;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import connection.database.GetConnection;
// Class dowload file từ server source về localhost
public class DownloadFile {
	// Class dùng để kiểm tra thư viện chilkat
	static { 
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Không tải được thư viện chilkat.\n" + e);
			System.exit(1);
		}
	}

	// Phương thức download file nhận vào tham số key từ table configuaration
	public void DownloadFie(int n) throws ClassNotFoundException, SQLException {
		// 1. Kết nối tới Database database_control
		Connection connection = new GetConnection().getConnection("control_db");
		// 2. Kết nối tới bảng data_file_configuaration
		String sql = "SELECT id, fileName, hostName, port, userName, passWord, remotePath, localPath FROM data_file_configuaration Where id=" + n;
		PreparedStatement ps = connection.prepareStatement(sql);
		// 3. Nhận resultSet chứa các record truy xuất
		ResultSet rs = ps.executeQuery();
		// 4. Duyệt record
		rs.next();
		String id = Integer.toString(rs.getInt("id"));
		String fileName = rs.getString("fileName");
		String hostName = rs.getString("hostName");
		int port = rs.getInt("port");
		String userName = rs.getString("userName");
		String passWord = rs.getString("passWord");
		String remotePath = rs.getString("remotePath");
		String localPath = rs.getString("localPath");
		
		// In ra màn hình kiểm tra 
		System.out.println(id + "-" + fileName);

		// 5. Kết nối và xác thực đến Server Cource
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Download");
		// Kết nối Server Cource với hostName: drive.ecepvn.org và port: 2227
		boolean success = ssh.Connect(hostName, port);
		if (success != true) {
			// 5.1 In thông báo lỗi kết nối ra màn hình
			System.out.println("Kết nối đến server source bị lỗi...");
			// 5.2 Gửi mail thông báo lỗi kết nối
			SendMail.sendMail("17130117@st.hcmuaf.edu.vn", "Data Warehouse", id + " " + fileName + " bị lỗi kết nối đến server");
			return;
		}
		// Chờ 5 giây khi nhận phản hồi
		ssh.put_IdleTimeoutMs(5000);
		// Đăng nhập vào Server Cource với userName: guest_access và passWord: 123456
		success = ssh.AuthenticatePw(userName, passWord);
		// In thông báo đăng nhập
		System.out.println("Đăng nhập thành công");
		if (success != true) {
			System.out.println("Đăng nhập thất bại");
			return;
		}
		// Tạo đối tượng SCP để tải file
		CkScp scp = new CkScp();
		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		// 6. Gọi hàm Download để tải file
		// Tải tất cả file bắt đầu tên fileName: sinhvien*.* (nhận vào tên file có kí tự đại diện)
		scp.put_SyncMustMatch(fileName + "*.*");
		// Tạo folder tên fileName
		localPath += "/" + fileName;
		// Tải xuống tất cả các file có tên sinhvien*.*
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		// 7. In thông báo tải file ra màn hình
		// 7.1 Nếu file tải thành công
		System.out.println("Tải thành công");

		// Kiểm tra tải file nếu tải không thành công
		if (success != true) {
			// 7.2.1 In thông báo lỗi ra màn hìn
			System.out.println(id + ": " + fileName + ": " + " Bị lỗi...");
			// 7.2.2 Gửi mail thông báo lỗi
			SendMail.sendMail("17130117@st.hcmuaf.edu.vn", "Warehouse", id + " " + fileName + ": Tải bị lỗi");
			return;
		}
		// 8. Ngắt kết nối đến server
		ssh.Disconnect();
		// Lấy danh sách file tải trong local
		List<File> listFile = listFile(localPath);
		// 9. Kiểm tra file tải
		checkFile(id, listFile, fileName);
		// 10. Đóng kết nối
		connection.close();

	}

	// Lấy danh sách file trong folder local đã tải về
	public List<File> listFile(String localPath) {
		File folder = new File(localPath);
		List<File> listFile = new ArrayList<File>();
		String[] paths = folder.list();
		for (int i = 0; i < paths.length; i++) {
			listFile.add(new File(localPath + File.separator + paths[i]));
			System.out.println(listFile.get(i));
		}
		return listFile;
	}

	// Kiểm tra file và ghi vào log
	public void checkFile(String id, List<File> listFile, String fileName) throws SQLException {
		Connection connection = new GetConnection().getConnection("control_db");
		for (int i = 0; i < listFile.size(); i++) {
			File f = listFile.get(i);
			
			// 9.1 Nếu file tải thành công
			if (f.length() > 0) {
				// Kiểm tra tên file đã tồn tại trong table log chưa
				if(checkLog(f.getName())) {
				// 9.1.1 Ghi lai log
				String log = "Insert into data_file_logs(id_config, your_fileName, table_staging, status_file, time_download) values ('"
						+ id + "','" + f.getName() + "','" + fileName + "','Download ok',NOW()) ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
				// 9.1.2 In thông báo ra màn hình
				System.out.println((i + 1) + ": " + f.getName() + ": " + "Đã ghi vào table_log...");
			}
			}
			// 9.2 Nếu file tải bị lỗi
			else {
				// 9.2.1 In thông báo lỗi ra màn hình
				System.out.println((i + 1) + ": " + f.getName() + ": " + " Bị lỗi...");
				// 9.2.2 Gửi mail thông báo lỗi
				SendMail.sendMail("17130117@st.hcmuaf.edu.vn", "Warehouse", (i + 1) + " " + f.getName() + ": Bị lỗi");
				// 9.2.3 Ghi lại log file bị lỗi
				String log = "Insert into data_file_logs(id_config,your_fileName, table_staging, status_file, time_download) values ('"
						+ id + "','" + f.getName()+ "','" + fileName  + "','Download error',NOW()) ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
			}

		}
		connection.close();
	}
	// Kiểm tra tên file đã tồn tại trong table log chưa
	public boolean checkLog(String fileName) throws SQLException {
//		System.out.println("File Name: "+ fileName);
		Connection connection = new GetConnection().getConnection("control_db");
		String sql = "SELECT your_filename FROM data_file_logs";
		PreparedStatement ps = connection.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
		String fileNameLog = rs.getString("your_filename");
//		System.out.println("fileName_Log: "+fileNameLog);
		if(fileName.equals(fileNameLog)) {
			return false;
		}
		}
		connection.close();
		return true;
		
	}

	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
		int n = 1;
		DownloadFile load = new DownloadFile();
		load.DownloadFie(n);
//		List<File> listFile =load.listFile("D:\\Github\\DataWarehouse_nhom14\\Project_DataWarehouse\\files\\monhoc");
//		load.checkFile("2", listFile, "monhoc");
		
	}
}
