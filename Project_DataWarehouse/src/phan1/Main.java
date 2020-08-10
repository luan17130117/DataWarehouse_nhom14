package phan1;

import java.sql.SQLException;
import java.util.Scanner;

public class Main {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Nhap so can tai: ");
		int n = sc.nextInt();
//		System.out.print("Bat dau tai: ");
		DownloadFileServer load = new DownloadFileServer();
		load.DownloadFile(n);
	}
}
