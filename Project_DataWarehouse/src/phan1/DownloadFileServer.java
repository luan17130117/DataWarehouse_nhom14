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
public class DownloadFileServer {
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
	// 1. Nhập key n cần tải trong table data_file_configuaration
	public void DownloadFile(int n) throws SQLException  {
	// 2. Kết nối tới Database database_control
		Connection connection = new GetConnection().getConnection("control");
	// 3. Kết nối tới bảng data_file_configuaration
		String sql = "SELECT id, fileName, hostName, port, userName, passWord, remotePath, localPath FROM data_file_configuaration Where id=" + n;
		PreparedStatement ps = connection.prepareStatement(sql);
	// 4. Nhận resultSet chứa các record truy xuất
		ResultSet rs = ps.executeQuery();
	// 5. Duyệt record
		if(rs.next()) {
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

	// 6. Kết nối và xác thực đến Server Source
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Download");
		// Kết nối Server Cource với hostName: drive.ecepvn.org và port: 2227
		boolean success = ssh.Connect(hostName, port);
		if (success != true) {
			// 6.1 In thông báo lỗi kết nối ra màn hình
			System.out.println("Kết nối đến server source bị lỗi...");
			// 6.2 Gửi mail thông báo lỗi kết nối
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
	// 7. Gọi hàm SyncTreeDownload(remotePath, localPath, 2, false) để tải file
		// Tải tất cả file bắt đầu tên fileName: sinhvien*.* (nhận vào tên file có kí tự đại diện)
		scp.put_SyncMustMatch(fileName + "*.*");
		// Tạo folder tên fileName
		localPath += "/" + fileName;
		// Tải xuống tất cả các file có tên sinhvien*.*
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
	// 8. In thông báo tải file thành công ra màn hình
		System.out.println("Tải thành công");
		// Kiểm tra tải file nếu tải không thành công
		if (success != true) {
			// 7.2.1 In thông báo lỗi ra màn hìn
			System.out.println(id + ": " + fileName + ": " + " Bị lỗi...");
			// 7.2.2 Gửi mail thông báo lỗi
			SendMail.sendMail("17130117@st.hcmuaf.edu.vn", "Warehouse", id + " " + fileName + ": Tải bị lỗi");
			return;
		}
		
	// 9. Ngắt kết nối đến server
		ssh.Disconnect();
		// Lấy danh sách file tải trong local
		List<File> listFile = listFile(localPath);
	// 10. Kiểm tra file tải và ghi vào bảng data_file_logs
		checkFile(id, listFile, fileName);
	// 11. Đóng kết nối
		connection.close();
		}else {
			System.out.println("Nhap sai. Vui long nhap lai...");
		}
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
		// Kết nối tới table data_file_logs trong database control
		Connection connection = new GetConnection().getConnection("control");
		for (int i = 0; i < listFile.size(); i++) {
			File f = listFile.get(i);
			// Kiểm tra tên file đã tồn tại trong table data_file_logs hay chưa
			if(checkLog(f.getName())) {
			// 10.1 Kiểm tra file tải về
			if (f.length() > 0) {
				// 10.1.1 Ghi vào log với trạng thái status_file=download ok
				String log = "Insert into data_file_logs(id_config, your_fileName, status_file, time_download) values ('"
						+ id + "','" + f.getName() + "','Download ok',NOW()) ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
				// 10.1.2 In thông báo ra màn hình
				System.out.println((i + 1) + ": " + f.getName() + ": " + "Đã ghi vào table_log...");
			}
			
			// 10.2 Nếu file tải bị lỗi
			else {
				// 10.2.1 In thông báo lỗi ra màn hình
				System.out.println((i + 1) + ": " + f.getName() + ": " + " Bị lỗi...");
				// 10.2.2 Gửi mail thông báo lỗi
				SendMail.sendMail("17130117@st.hcmuaf.edu.vn", "Warehouse", (i + 1) + " " + f.getName() + ": Bị lỗi");
				// 10.2.3 Ghi lại log file bị lỗi
				String log = "Insert into data_file_logs(id_config,your_fileName, status_file, time_download) values ('"
						+ id + "','" + f.getName()+ "', 'Download error',NOW()) ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
			}
		}
		}
		// Đóng kết nối
		connection.close();
	}
	// Kiểm tra tên file đã tồn tại trong bảng data_file_logs chưa
	public boolean checkLog(String fileName) throws SQLException {
		// Kết nối tới table data_file_logs trong database control
		Connection connection = new GetConnection().getConnection("control");
		String sql = "SELECT your_filename FROM data_file_logs";
		PreparedStatement ps = connection.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
		String fileNameLog = rs.getString("your_filename");
		// Kiểm tra tên file tồn tại
		if(fileName.equals(fileNameLog)) {
			return false;
		}
		}
		// Đóng kết nối
		connection.close();
		return true;
		
	}

	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
//		int n = 1;
		DownloadFileServer load = new DownloadFileServer();
//		load.DownloadFie(n);
		List<File> listFile =load.listFile("C:\\DevPrograms\\git\\DataWarehouse_nhom14\\Project_DataWarehouse\\files\\monhoc");
		load.checkFile("2", listFile, "monhoc");
		
	}
}
