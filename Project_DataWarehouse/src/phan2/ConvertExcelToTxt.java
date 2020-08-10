package phan2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import sun.java2d.pipe.SpanShapeRenderer.Simple;

public class ConvertExcelToTxt {

	public static void main(String[] args) throws IOException {
		final String excelFilePath = "C:\\DevPrograms\\git\\DataWarehouse_nhom14\\Project_DataWarehouse\\files\\sinhvien\\sinhvien_chieu_nhom14.xlsx";
		String txtFilePath = "C:\\DevPrograms\\git\\DataWarehouse_nhom14\\Project_DataWarehouse\\files\\convert\\sinhvien\\sinhvien_chieu_nhom14.txt";
		convertExcelToTxt(excelFilePath, txtFilePath, ";");
	}

	@SuppressWarnings("unused")
	public static void convertExcelToTxt(String excelFilePath, String txtFilePath, String delimiter)
			throws IOException {

		File txtFile = new File(txtFilePath);
		txtFile.createNewFile();

		StringBuffer data = new StringBuffer();
		FileOutputStream txtFileOut = new FileOutputStream(txtFile);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(txtFileOut, "UTF-8"));

		// Get file
		InputStream inputStream = new FileInputStream(new File(excelFilePath));
		// Get workbook
		Workbook workbook = getWorkbook(inputStream, excelFilePath);
		// Get sheet
		Sheet sheet = workbook.getSheetAt(0);

		int maxNumOfCells = sheet.getRow(0).getLastCellNum();
		// Get all rows
		Iterator<Row> iterator = sheet.iterator();

		while (iterator.hasNext()) {
			Row nextRow = iterator.next();

			for (int cellCounter = 0; cellCounter < maxNumOfCells; cellCounter++) { // Loop through cells
				if (nextRow.getCell(cellCounter) == null) {
					Cell newCell = nextRow.createCell(cellCounter);
				} else {
					Cell newCell = nextRow.getCell(cellCounter);
				}

			}

			// Get all cells
			Iterator<Cell> cellIterator = nextRow.cellIterator();

			// Read cells and set value for book object
			while (cellIterator.hasNext()) {
				// Read cell
				Cell cell = cellIterator.next();
				if (cell != null) {
					switch (cell.getCellTypeEnum()) {
					case BLANK:
					case _NONE:
					case ERROR:
						data.append(delimiter + "?");
						break;
					case STRING:
						data.append(delimiter + cell.getStringCellValue());
						break;
					case NUMERIC:
						if(HSSFDateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
							Date javaDate =  DateUtil.getJavaDate(cell.getNumericCellValue());
							String dateFormat = format.format(javaDate) ;
							data.append(delimiter + dateFormat);
						}else {
							data.append(delimiter + (int) cell.getNumericCellValue());
						}
						break;
					case BOOLEAN:
						data.append(delimiter + cell.getBooleanCellValue());
						break;
					case FORMULA:
						// In ra Công thức
						FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
						// In ra giá trị từ công thức
						data.append(delimiter + (int) evaluator.evaluate(cell).getNumberValue());
						break;
					default:
						data.append(delimiter + "?");
						break;
					}
				}
			}
			data.append('\n');
		}

		String perfectData = data.toString().replaceAll("\n" + delimiter, "\r\n"); // Convert LF to CRLF

		StringBuilder perfectDataSB = new StringBuilder(perfectData);
		perfectDataSB.deleteCharAt(0);

		System.out.println("[Convert] [" + excelFilePath + "] to [" + txtFilePath + "]");

		bw.write(perfectDataSB.toString());
		bw.flush(); //

		workbook.close();
		inputStream.close();
		bw.close();
	}

	// Get Workbook
	private static Workbook getWorkbook(InputStream inputStream, String excelFilePath) throws IOException {
		Workbook workbook = null;
		if (excelFilePath.endsWith("xlsx")) {
			workbook = new XSSFWorkbook(inputStream);
		} else if (excelFilePath.endsWith("xls")) {
			workbook = new HSSFWorkbook(inputStream);
		} else {
			throw new IllegalArgumentException("The specified file is not Excel file " + excelFilePath);
		}
		return workbook;
	}
}
