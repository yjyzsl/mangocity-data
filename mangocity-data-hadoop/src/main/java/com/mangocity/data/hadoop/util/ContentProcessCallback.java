package com.mangocity.data.hadoop.util;

import java.io.InputStream;

public interface ContentProcessCallback {
	void processContent(final String parent, final String path,
			final InputStream in, int index);
}