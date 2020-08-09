package main;

import java.sql.SQLException;

import phan1.DownloadFile;
import phan2.Staging;
import phan3.DataWarehouseClass;
import phan3.DataWarehouseRegistration;
import phan3.DataWarehouseStudent;
import phan3.DataWarehouseSubject;

public class Main2 {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		for (int i = 0; i < 4; i++) {
			DownloadFile load = new DownloadFile();
			load.DownloadFie(i);
			new Staging().loadStudentToStaging();
			if(i == 0) {
				new DataWarehouseStudent().insertStudentToDW();
			}
			if(i == 1) {
				new DataWarehouseSubject().insertSubjectToDW();
			}
			if(i == 2) {
				new DataWarehouseClass().insertClassToDW();
			}
			if(i == 3) {
				new DataWarehouseRegistration().insertRegistrationToDW();
			}
		}
	}
}
