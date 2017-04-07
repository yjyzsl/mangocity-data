package com.mangocity.data.hadoop.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;




import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;

public abstract class VfsUtils {
	static final FileSystemOptions options = new FileSystemOptions();
	static {
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, false);
	}
	
	public static String getFileName(String path) {
		try {
			FileSystemManager fsManager = VFS.getManager();
			SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, false);
			FileObject fo = null;
			fo = fsManager.resolveFile(path, options);
			return fo.getName().getBaseName();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static InputStream readFileContent(String path) {
		try {
			FileSystemManager fsManager = VFS.getManager();
			FileSystemOptions options = new FileSystemOptions();
			SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, false);
			FileObject fo = null;
			fo = fsManager.resolveFile(path, options);
			return fo.getContent().getInputStream();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static void readFileContentBySplit(String path, ContentReaderCallback callback) {
		readFileContentBySplit(path, callback, null);
	}
	
	public static void readFileContentBySplit(String path, ContentReaderCallback callback, Logger log) {
		readFileContentBySplit(path, callback, null, null);
	}
	
	public static void readFileContentBySplit(String path, ContentReaderCallback callback, Logger log, String encoding) {
		if (StringUtils.isBlank(encoding)) {
			encoding = "UTF-8";
		}
		InputStream is = null;
		BufferedReader br = null;
		try {
			is = readFileContent(path);
			br = new BufferedReader(new InputStreamReader(is, encoding));
			int splitSize = callback.getSplitSize();
			splitSize = (splitSize <=0 ) ? 1 : splitSize;
			int lineNo = 0, currentSplit = 0;
			String line = null;
			StringBuffer sb = new StringBuffer();
			while ((line=br.readLine()) != null) {
				lineNo++;
				sb.append(line).append("\n");
				if (lineNo%splitSize==0) {
					try {
						callback.processContent(sb.toString());
						currentSplit++;
						sb.setLength(0);
					} catch (Exception e) {
						if (null != log) {
							log.error("处理文件分片错误->{},->{}", path, currentSplit);
							log.error("处理文件分片错误:"+path+", "+e.getMessage(), e);
						} else {
							e.printStackTrace();
						}
					}
				}
			}
			if (sb.length() > 0) {
				try {
					callback.processContent(sb.toString());
					currentSplit++;
				} catch (Exception e) {
					if (null != log) {
						log.error("处理文件分片错误->{},->{}", path, currentSplit);
						log.error("处理文件分片错误:"+path+", "+e.getMessage(), e);
					} else {
						e.printStackTrace();
					}
				}
			}
			sb.setLength(0);
		} catch (Exception e) {
			if (null != log) {
				log.error("读取文件内容出错->{},->{}", path);
				log.error("读取文件内容出错:"+path+", "+e.getMessage(), e);
			} else {
				e.printStackTrace();
			}
		} finally {
			IOUtils.closeQuietly(is);
		}
		
	}
	
	public static String readFileToString(String path) {
		return readFileToString(path, "UTF-8");
	}
	
	public static String readFileToString(String path, String charset) {
		StringWriter sw = new StringWriter();
		InputStream is = null;
		try {
			is = readFileContent(path);
			IOUtils.copy(is, sw, charset);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (Exception e) {
			}
		}
		return sw.getBuffer().toString();
	}
	
	public static void readFileContent(String path, ContentProcessCallback callback) {
		try {
			FileSystemManager fsManager = VFS.getManager();
			FileSystemOptions options = new FileSystemOptions();
			SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, false);
			FileObject fo = null;
			fo = fsManager.resolveFile(path, options);
			if (fo.getType() == FileType.FILE) {
				callback.processContent(fo.getParent().getURL().toString(), path, fo.getContent().getInputStream(), 0);
			} else if (fo.getType() == FileType.FOLDER) {
				FileObject[] children = fo.getChildren();
				if (null != children && children.length > 0) {
					for (int i=0; i<children.length; i++) {
						FileObject fos = children[i];
						if (fos.getType() == FileType.FILE) {
							callback.processContent(fos.getParent().getURL().toString(), fos.getURL().toExternalForm(), fos.getContent().getInputStream(), i);
						} else {
							continue;
						}
					}
				} else {
					throw new RuntimeException("没有输入数据文件");
				}
			} else {
				throw new RuntimeException("文件既不是目录也不是文件:"+path);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static List<String> readFileContentLines(final String path, final String charset, final Logger log) {
		final List<String> dataLines = new ArrayList<String>();
		try {
			VfsUtils.readFileContent(path, new ContentProcessCallback() {
				@Override
				public void processContent(String parent, String path, InputStream in, int index) {
					try {
						List<String> lines = IOUtils.readLines(in, charset);
						dataLines.addAll(lines);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		} catch (Exception e) {
			log.error("读取文件错误", e);
		}
		return dataLines;
	}
	
	public static void main(String[] args) {
		System.out.println(readFileToString("http://www.mangocity.com/lvyou/beijing/"));
	}
}
