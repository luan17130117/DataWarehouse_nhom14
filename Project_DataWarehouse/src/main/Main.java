package main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

import connection.database.GetConnection;
import phan1.DownloadFile;
import phan2.Staging;
import phan3.DataWarehouseClass;
import phan3.DataWarehouseRegistration;
import phan3.DataWarehouseStudent;
import phan3.DataWarehouseSubject;

public class Main {
	public Main() {

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		while (true) {
			System.out.println("1. Nhap so can tai\n 0. Exit");
			Scanner sc = new Scanner(System.in);
			int value = sc.nextInt();
			if (value == 1) {
				DownloadFile load = new DownloadFile();
				load.DownloadFile(value);
				new Staging().loadStudentToStaging();
				new DataWarehouseStudent().insertStudentToDW();
			} else if (value == 2) {
				DownloadFile load = new DownloadFile();
				load.DownloadFile(value);
				new Staging().loadStudentToStaging();
				new DataWarehouseSubject().insertSubjectToDW();
			} else if (value == 3) {
				DownloadFile load = new DownloadFile();
				load.DownloadFile(value);
				new Staging().loadStudentToStaging();
				new DataWarehouseClass().insertClassToDW();
			} else if (value == 4) {
				DownloadFile load = new DownloadFile();
				load.DownloadFile(value);
				new Staging().loadStudentToStaging();
				new DataWarehouseRegistration().insertRegistrationToDW();
			} else if (value == 0) {
				System.out.println("Hẹn gặp lại sau!");
				break;
			} else {
				System.out.println("Nhập không đúng số trong danh mục!");
			}
		}
	}

}
