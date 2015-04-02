package algo.ad.eval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import jxl.Cell;
import jxl.CellType;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

class ReadExcel {

	final int REF_POSITIVE = 20;
	final int REF_NEUTRAL = 17;
	final int REF_NEGATIVE = 19;

	private String inputFile;
	private FMeasure f_measure;

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;

	}

	public ReadExcel() {
		super();
		// TODO Auto-generated constructor stub
		f_measure = new FMeasure();
	}

	public void read() throws IOException {
		File inputWorkbook = new File(inputFile);
		Workbook w;
		try {
			w = Workbook.getWorkbook(inputWorkbook);
			// Get the first sheet
			Sheet sheet_EWMA_STD_DEV = w.getSheet(0);
			Sheet sheet_EWMA_MAD = w.getSheet(1);
			Sheet sheet_PEWMA_STD_DEV = w.getSheet(2);
			Sheet sheet_PEWMA_MAD = w.getSheet(3);

			// Loop over first 10 column and lines

			perform_experiment(sheet_EWMA_STD_DEV);

			perform_experiment(sheet_EWMA_MAD);
			perform_experiment(sheet_PEWMA_STD_DEV);
			perform_experiment(sheet_PEWMA_MAD);
		} catch (BiffException e) {
			e.printStackTrace();
		}
	}

	void perform_experiment(Sheet sheet) {
		ArrayList<Double> fscores_avg_list = new ArrayList<Double>();
		System.out.println("*******************************************");
		System.out.println(sheet.getName());
		System.out.println("*******************************************");
		for (int i = 0; i < sheet.getRows(); i++) {
			Cell alpha_LO = sheet.getCell(0, i);
			Cell alpha_WO = sheet.getCell(1, i);

			Cell positive_selected = sheet.getCell(2, i);
			Cell neutral_selected = sheet.getCell(4, i);
			Cell negative_selected = sheet.getCell(6, i);

			Cell positive_true = sheet.getCell(3, i);
			Cell neutral_true = sheet.getCell(5, i);
			Cell negative_true = sheet.getCell(7, i);

			CellType type = alpha_LO.getType();
			if (type != CellType.LABEL && positive_selected.getContents() != "") {

				// System.out.println("*******************************************");
//				double positive_fScore = getFScore(
//						Integer.parseInt(alpha_LO.getContents()),
//						Integer.parseInt(alpha_WO.getContents()),
//						REF_POSITIVE,
//						Integer.parseInt(positive_selected.getContents()),
//						Integer.parseInt(positive_true.getContents()));
//
//				double neutral_fScore = getFScore(
//						Integer.parseInt(alpha_LO.getContents()),
//						Integer.parseInt(alpha_WO.getContents()), REF_NEUTRAL,
//						Integer.parseInt(neutral_selected.getContents()),
//						Integer.parseInt(neutral_true.getContents()));
//
//				double negative_fScore = getFScore(
//						Integer.parseInt(alpha_LO.getContents()),
//						Integer.parseInt(alpha_WO.getContents()), REF_NEGATIVE,
//						Integer.parseInt(negative_selected.getContents()),
//						Integer.parseInt(negative_true.getContents()));
				
				
				double accum_fScore = getFScore(
						Integer.parseInt(alpha_LO.getContents()),
						Integer.parseInt(alpha_WO.getContents()),
						
						REF_POSITIVE + REF_NEUTRAL + REF_NEGATIVE,
						
						Integer.parseInt(positive_selected.getContents())
								+ Integer.parseInt(neutral_selected
										.getContents())
								+ Integer.parseInt(negative_selected
										.getContents()),
										
						Integer.parseInt(positive_true.getContents())
								+ Integer.parseInt(neutral_true.getContents())
								+ Integer.parseInt(negative_true.getContents()));

				//double avg_fscore = (positive_fScore + neutral_fScore + negative_fScore) / 3;
				fscores_avg_list.add(accum_fScore);
//				 System.out.println("config: LO:WO = " +
//				 Integer.parseInt(alpha_LO.getContents()) + ":" +
//				 Integer.parseInt(alpha_WO.getContents())
//				 + "| accum_fscore:" + accum_fScore);
				 
				//System.out.println(accum_fScore);
			}
		}

		Collections.sort(fscores_avg_list);

		System.out.println("Best Accum f-score: "
				+ fscores_avg_list.get(fscores_avg_list.size() - 1));
		System.out.println("");
	}

	double getFScore(int alpha_LO, int alpha_WO, int target, int selected,
			int true_positive) {
		double fScore = 0;

		f_measure.setTarget(target);
		f_measure.setSelected(selected);
		f_measure.setTruePositive(true_positive);

		fScore = f_measure.getFMeasure();
		 System.out.println("config: LO:WO = " + alpha_LO + ":" + alpha_WO
		+"| Precesion:"+f_measure.getPrecisionScore()+"| Recall:"+f_measure.getRecallScore() + "| fscore:" + fScore);

		return fScore;
	}
}

public class Experiments {
	public static void main(String[] args) throws IOException {
		ReadExcel test = new ReadExcel();
		test.setInputFile("D:/BitSync Work Folder/Paper/latex/Evaluation/Dataset/Fscore_eval_sheet.xls");
		test.read();
	}
}