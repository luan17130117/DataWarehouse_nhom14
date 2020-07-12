package upload;

import java.io.BufferedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;

public class FTPUploader {

	public void upload(String ftp, String port, String user, String pass, String src, String des) {
		FTPClient ftpClient = new FTPClient();
		InputStream is = null;
		try {
			ftpClient.connect(ftp, Integer.parseInt(port));
			boolean isLogin = ftpClient.login(user, pass);
			if (!isLogin) {
				System.out.println("Can't login !");
			} else {
				is = new BufferedInputStream(new FileInputStream(src));
				ftpClient.enterLocalPassiveMode();
				ftpClient.changeWorkingDirectory(des);
				boolean isSaved = ftpClient.storeFile(src, is);
				if (isSaved) {
					System.out.println("Your file is saved on FtpServer !");
					// notify clients at here
				}
			}
			is.close();
			ftpClient.logout();
		} catch (IOException io) {
			System.out.println("UploadFileToFTPServer: " + io.getMessage());
		}
	}

	public static void main(String[] args) {

		String host = "", port = "", user = "", pass = "", src = "", des = "";
		if (args.length != 12) {
			System.out.println("Missing params !");
		} else {
			if ("-host".equals(args[0])) {
				host = args[1].trim();
			}
			if ("-port".equals(args[2])) {
				port = args[3].trim();
			}
			if ("-user".equals(args[4])) {
				user = args[5].trim();
			}
			if ("-pass".equals(args[6])) {
				pass = args[7].trim();
			}
			if ("-src".equals(args[8])) {
				src = args[9].trim();
			}
			if ("-des".equals(args[10])) {
				des = args[11].trim();
			}
			new FTPUploader().upload(host, port, user, pass, src, des);
		}
	}
}
