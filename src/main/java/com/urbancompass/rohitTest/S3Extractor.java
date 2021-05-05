/**
 * Copyright (C) 2021 Urban Compass, Inc.
 */
package com.urbancompass.rohitTest;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author rohit.kommareddy
 */
public class S3Extractor {

	
	private static final String S3_BUCKET = "compass-production-elastic-search-queries";
	private static final String S3_PREFIX = "queries/year=2021/month=04/day=%s/hour=%s/";
	
	private static AmazonS3 s3 = null;
	
	public static void process(int day, int fromHour, int toHour) throws Exception {

		s3 = AmazonS3ClientBuilder.standard().build();
		if (fromHour == -1) {
			fromHour = 0;
		}
		if (toHour == -1) {
			toHour = 23;
		}
		List<String> files = new ArrayList<>();
		for (int i = fromHour; i <= toHour; i++) {
			files.addAll(getFilesForHour(day, i));
		}
		
		System.out.print(files.size());

	}

	private static List<String> getFilesForHour(int day, int hour) {
		List<String> files = new ArrayList<>();
		String prefix = String.format(S3_PREFIX, String.format("%02d", day), String.format("%02d", hour));
		System.out.println(prefix);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(S3_BUCKET).withPrefix(prefix);
		ObjectListing listObjects = null;
		do {
			listObjects = s3.listObjects(listObjectsRequest);
			for (S3ObjectSummary summary: listObjects.getObjectSummaries()) {
				System.out.println(summary.getKey());
				files.add(summary.getKey());
			}
			listObjectsRequest.setMarker(listObjects.getNextMarker());
		} while (listObjects.isTruncated());
		
		return files;
	}

}
