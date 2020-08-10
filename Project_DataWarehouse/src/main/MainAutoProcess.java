package main;

import java.sql.SQLException;

import phan1.DownloadFileServer;
import phan2.Staging;
import phan3.DataWarehouseClass;
import phan3.DataWarehouseRegistration;
import phan3.DataWarehouseStudent;
import phan3.DataWarehouseSubject;

public class MainAutoProcess {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		for (int i = 1; i <= 4; i++) {
			DownloadFileServer load = new DownloadFileServer();
			if(i == 1) {
				load.DownloadFile(i);
				new Staging().loadStudentToStaging();
				new DataWarehouseStudent().insertStudentToDW();
			}
			if(i == 2) {
				load.DownloadFile(i);
				new Staging().loadStudentToStaging();
				new DataWarehouseSubject().insertSubjectToDW();
			}
			if(i == 3) {
				load.DownloadFile(i);
				new Staging().loadStudentToStaging();
				new DataWarehouseClass().insertClassToDW();
			}
			if(i == 4) {
				load.DownloadFile(i);
				new Staging().loadStudentToStaging();
				new DataWarehouseRegistration().insertRegistrationToDW();
			}
		}
	}
}
