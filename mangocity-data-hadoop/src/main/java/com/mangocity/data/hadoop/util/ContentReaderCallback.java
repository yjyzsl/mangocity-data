package com.mangocity.data.hadoop.util;

public interface ContentReaderCallback {

	int getSplitSize();
	
	void processContent(String splitLines);
}
