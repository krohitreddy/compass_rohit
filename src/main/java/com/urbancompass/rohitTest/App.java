package com.urbancompass.rohitTest;

public class App {

	public static void main(String[] args) {
		
		try {
			S3Extractor.process(29, -1, -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
