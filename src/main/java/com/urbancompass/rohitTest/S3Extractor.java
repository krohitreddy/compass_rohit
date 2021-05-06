/**
 * Copyright (C) 2021 Urban Compass, Inc.
 */
package com.urbancompass.rohitTest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * @author rohit.kommareddy
 */
public class S3Extractor {

	private static final String S3_BUCKET = "compass-production-elastic-search-queries";
	private static final String S3_PREFIX = "queries/year=2021/month=04/day=%s/hour=%s/";

	private static AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	

	public static void copyFilesToLocal(int day, int fromHour, int toHour) throws Exception {
		if (fromHour == -1) {
			fromHour = 0;
		}
		if (toHour == -1) {
			toHour = 23;
		}
		
		List<String[]> files = new ArrayList<>();
		for (int i = fromHour; i < toHour; i++) {
			files.addAll(getFilesWithTSForHour(day, i));
		}
		
		System.out.println("size: " + files.size());
		
		int num = 1;
		for (String[] arr: files) {
			System.out.println(num + "");
			System.out.println(arr[0]);
			num++;
			S3ObjectInputStream objectContent = null;
			OutputStream outputStream = null;
			String filePath = "/home/rohit.kommareddy/development/rohitTest/files/" + arr[1] + ".parquet";
			File tempFile = new File(filePath);
			
			try {
				S3Object s3Object = s3.getObject(S3_BUCKET, arr[0]);
				objectContent = s3Object.getObjectContent();
				outputStream = new FileOutputStream(tempFile);
				IOUtils.copyLarge(objectContent, outputStream);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			} finally {
				if (objectContent != null) {
					try {
						objectContent.close();
					} catch (IOException e) {
					}
				}
				if (outputStream != null) {
					try {
						outputStream.close();
					} catch (IOException e) {
					}
				}
			}
		}
	}
	
	private static List<String[]> getFilesWithTSForHour(int day, int hour) {
		List<String[]> files = new ArrayList<>();
		String prefix = String.format(S3_PREFIX, String.format("%02d", day), String.format("%02d", hour));
		System.out.println(prefix);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(S3_BUCKET).withPrefix(prefix);
		ObjectListing listObjects = null;
		do {
			listObjects = s3.listObjects(listObjectsRequest);
			for (S3ObjectSummary summary : listObjects.getObjectSummaries()) {
				// System.out.println(summary.getKey());
				long ts = summary.getLastModified().getTime();
				files.add(new String[] {summary.getKey(), Long.toString(ts)});
			}
			listObjectsRequest.setMarker(listObjects.getNextMarker());
		} while (listObjects.isTruncated());

		return files;
	}
	
	
	public static void getFilesAndExtractData(int day, int fromHour, int toHour) throws Exception {

		if (fromHour == -1) {
			fromHour = 0;
		}
		if (toHour == -1) {
			toHour = 24;
		}
		List<String> files = new ArrayList<>();
		for (int i = fromHour; i < toHour; i++) {
			files.addAll(getFilesForHour(day, i));
		}

		
		System.out.println("total files: " + files.size());
		getData(files);

	}

	private static void getData(List<String> files) throws Exception {
		BufferedWriter writer = new BufferedWriter(new FileWriter("/home/rohit.kommareddy/development/rohitTest/finalFile.tsv"));
		writer.write("timestamp\tquery");
		writer.newLine();
		int num = 1;
		for (String file : files) {
			System.out.println(num + "");
			System.out.println(file);
			num++;
			S3ObjectInputStream objectContent = null;
			OutputStream outputStream = null;
			String filePath = "/home/rohit.kommareddy/development/rohitTest/" + UUID.randomUUID().toString();
			File tempFile = new File(filePath);
			try {
				S3Object s3Object = s3.getObject(S3_BUCKET, file);
				objectContent = s3Object.getObjectContent();
				outputStream = new FileOutputStream(tempFile);
				IOUtils.copyLarge(objectContent, outputStream);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			} finally {
				if (objectContent != null) {
					try {
						objectContent.close();
					} catch (IOException e) {
					}
				}
				if (outputStream != null) {
					try {
						outputStream.close();
					} catch (IOException e) {
					}
				}
			}

			try {
				List<SimpleGroup> simpleGroups = new ArrayList<>();
				ParquetFileReader reader = ParquetFileReader
						.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
				MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		        
		        PageReadStore pages;
		        while ((pages = reader.readNextRowGroup()) != null) {
		        	long rows = pages.getRowCount();
		        	System.out.println("rows: " +  rows);
		        	MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
		            RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
		            
		            for (int i = 0; i < rows; i++) {
		            	SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
		            	simpleGroups.add(simpleGroup);
		            	
		            }
		        }
		        
		        
		        reader.close();
		        tempFile.delete();
		        
		        for (int i = 0; i < simpleGroups.size(); i++) {
		        	writer.write(simpleGroups.get(i).getLong("time_millis", 0) + "");
		        	writer.write("\t");
		        	writer.write(simpleGroups.get(i).getString("query", 0) + "");
		        	writer.newLine();
		        }
		        
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		writer.close();
	}

	private static List<String> getFilesForHour(int day, int hour) {
		List<String> files = new ArrayList<>();
		String prefix = String.format(S3_PREFIX, String.format("%02d", day), String.format("%02d", hour));
		System.out.println(prefix);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(S3_BUCKET).withPrefix(prefix);
		ObjectListing listObjects = null;
		do {
			listObjects = s3.listObjects(listObjectsRequest);
			for (S3ObjectSummary summary : listObjects.getObjectSummaries()) {
				// System.out.println(summary.getKey());
				files.add(summary.getKey());
			}
			listObjectsRequest.setMarker(listObjects.getNextMarker());
		} while (listObjects.isTruncated());

		return files;
	}

}
