package com.urbancompass.rohitTest;

import java.io.BufferedReader;
import java.io.FileReader;

public class App {

	public static void main(String[] args) {

		try {
			// S3Extractor.process(29, -1, -1);
			justCount();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void justCount() {
		try {
			BufferedReader reader = new BufferedReader(
					new FileReader("/home/rohit.kommareddy/development/rohitTest/finalFile.tsv"));
			System.out.println(reader.lines().count());
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
