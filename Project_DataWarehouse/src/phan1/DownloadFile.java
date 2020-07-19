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

public class DownloadFile {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	//
	public void DownloadFie(int n) throws ClassNotFoundException, SQLException {
		// 1. Kết nối tới Database Control_DB
		Connection connection = new GetConnection().getConnection("control_db");
		// 2. Kết nối tới bảng table_config
		String sql = "SELECT * FROM table_config Where id="+n;
		PreparedStatement ps = connection.prepareStatement(sql);
		// 3. Nhận resultSet chứa các record truy xuất
		ResultSet rs = ps.executeQuery();
		// 4. Duyệt record
		while (rs.next()) {
			String id = Integer.toString(rs.getInt("id"));
			String fileName = rs.getString("fileName");
			String hostName = rs.getString("hostName");
			int port = rs.getInt("port");
			String userName = rs.getString("userName");
			String passWord = rs.getString("passWord");
			String srcPath = rs.getString("remotePath");
			String localPath = rs.getString("localPath");
			
			System.out.println(id+"-"+fileName);

		// 5. Kết nối đến Server Cource
			CkSsh ssh = new CkSsh();
			CkGlobal ck = new CkGlobal();
			ck.UnlockBundle("Download");
			boolean success = ssh.Connect(hostName, port);
			if (success != true) {
				// 5.1 In thông báo lỗi ra màn hình
				System.out.println("Kết nối đến server cource bị lỗi...");
				// 5.2 Gửi mail thông báo lỗi
				SendMail.sendMail("test@st.hcmuaf.edu.vn", "Warehouse",id+" "+fileName+ " bị lỗi kết nối đến server");
				return;
			}
			ssh.put_IdleTimeoutMs(5000);
			//
			success = ssh.AuthenticatePw(userName, passWord);
			System.out.println("Đã đăng nhập");
			if (success != true) {
				System.out.println("Đăng nhập thất bại");
				return;
			}
			//
			CkScp scp = new CkScp();
			success = scp.UseSsh(ssh);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return;
			}
		// 6. Gọi hàm DownloadFile để tải file
			// Tải tất cả file bắt đầu tên fileName
			scp.put_SyncMustMatch(fileName+"*.*");
			// Tạo folder tên fileName
			localPath+="/"+fileName;
			success = scp.SyncTreeDownload(srcPath, localPath, 2, false);
		// 7. In thông báo tải file thành công ra màn hình
			System.out.println("Tải thành công");
			
		
			//Kiểm tra tải file nếu tải không thành công
			if (success != true) {
				// 8.1 In thông báo lỗi ra màn hìn
				System.out.println(id+": "+fileName+": "+" Bị lỗi...");
				// 8.2 Gửi mail thông báo lỗi
				SendMail.sendMail("test@st.hcmuaf.edu.vn", "Warehouse",id+" "+ fileName+": Bị lỗi");
				return;
			}
			// Ngắt kết nối server
			ssh.Disconnect();
			// 
			List<File> listFile= listFile(localPath);
			// Kiểm tra file tải
			checkFile(id, listFile);
		}	
		// 11. Đóng kết nối
		connection.close();

	}
	//
	public  List<File> listFile(String dir) {
		 File directoryPath = new File(dir);
		 List<File> listFile = new ArrayList<File>();
		 String[] paths = directoryPath.list();
		 for (int i = 0; i < paths.length; i++) {
			listFile.add(new File(dir + File.separator + paths[i]));
			System.out.println(listFile.get(i));
		 }
		 return listFile;
	}
	//
	public void checkFile(String id,List<File> listFile) throws SQLException {
		Connection connection = new GetConnection().getConnection("control_db");
		for (int i = 0; i < listFile.size(); i++) {
			File f = listFile.get(i);
			if(f.length()>0) {
				// 8. Ghi lai log
				String log = "Insert into table_log(id, fileName, status) values ('"+(i+1)+"','" +f.getName() +"','Download OK') ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
				System.out.println((i+1)+": "+f.getName()+": "+"Đã ghi vào table_log...");
			}
			else {
				// In thông báo lỗi ra màn hình
				System.out.println((i+1)+": "+f.getName()+": "+" Bị lỗi...");
				// Gửi mail thông báo lỗi
				SendMail.sendMail("test@st.hcmuaf.edu.vn", "Warehouse", (i+1)+" "+f.getName()+": Bị lỗi");
				// Ghi lại log file bị lỗi
				String log = "Insert into table_log(id, fileName, status) values ('"+(i+1)+"','" +f.getName() +"','Download ERROR') ";
				PreparedStatement pslog = connection.prepareStatement(log);
				pslog.executeUpdate(log);
			}
		}
		connection.close();
	}
	
	
	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
		int n =1;
		DownloadFile load = new DownloadFile();
		load.DownloadFie(n);
	}
}
