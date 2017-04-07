package com.mangocity.data.commons.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.tool.SpecificCompilerTool;
import org.apache.avro.tool.Tool;

/**
 *
 * @author shilei
 * @date 2016年5月30日 下午2:08:12 
 */
public class GenerateAvroJavaFile {
	
	public static void generateAvroJavaFile(){
		String resourcePath = GenerateAvroJavaFile.class.getClassLoader().getResource("").getPath();
		File file = new File(resourcePath);
		File[] resourcesFile = file.listFiles();
		String fileName = null;
		String filePath = null;
		String javaFilePath = System.getProperty("user.dir")+File.separator+"src"+File.separator+"main"+File.separator+"java"; 
		Tool tool = new SpecificCompilerTool();
		for (File resource : resourcesFile) {
			fileName = resource.getName();
			if(!fileName.endsWith(".avsc")){
				continue;
			}
			filePath = resource.getAbsolutePath();
			try {
				List<String> arr = new ArrayList<String>();
				arr.add("schema");
				arr.add(filePath);
				arr.add(javaFilePath);
				tool.run(System.in, System.out, System.err, arr);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		generateAvroJavaFile();
	}
	

}

