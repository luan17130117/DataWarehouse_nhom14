

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class demo {
	private static Connection con;
	static String host;
	static String username;
	static String password;
	static String remotePath;
	static String localPath;

	private Connection connect;
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void createConnection() throws SQLException {

		System.out.println("Connecting database....");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/dataWH?useUnicode=true&characterEncoding=utf-8", "root", "");

			System.out.println("Complete!!!!!");
			System.out.println("----------------------------------------------------------------");
		} catch (ClassNotFoundException e) {
			System.out.println("Can't connect!!!!!!!!!!");
			System.out.println("----------------------------------------------------------------");
		}
	}

	public void getConfig() throws SQLException {

		PreparedStatement st = (PreparedStatement) con.prepareStatement("SELECT * from config");
		ResultSet rs = st.executeQuery();
		rs.next();
		host = rs.getString("hostname");
		username = rs.getString("user");
		password = rs.getString("pass");
		remotePath = rs.getString("remotePath");
		localPath = rs.getString("localPath");
	}

	public void dowloadfile() {
		CkSsh ckssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		CkScp scp = new CkScp();
		ck.UnlockBundle("Ä�áº·ng Thá»‹ PhÆ°á»£ng");
		int port = 2227;
		boolean success = ckssh.Connect(host, port);
		if (success != true) {
			System.out.println(ckssh.lastErrorText());
			return;
		}
		ckssh.put_IdleTimeoutMs(5000);
		success = ckssh.AuthenticatePw(username, password);
		if (success != true) {
			System.out.println(ckssh.lastErrorText());
			return;
		}

		success = scp.UseSsh(ckssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch("");

		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println("Dowload fail...........");
			return;
		}
		System.out.println("SCP download file success.");
		ckssh.Disconnect();

	}
	

	public static void main(String argv[]) throws ClassNotFoundException, SQLException, IOException {
		demo dm = new demo();
//		te.createConnection();
		dm.getConfig();
		dm.dowloadfile();
//		te.writeLog("D://data");
	}
}
