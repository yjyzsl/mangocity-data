package com.mangocity.data.commons;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.avro.tool.SpecificCompilerTool;
import org.apache.avro.tool.Tool;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	public void generateAvroJavaFile(){
		Tool tool = new SpecificCompilerTool();
	      if (tool != null) {
	    	  List<String> arr = new ArrayList<String>();
	    	  arr.add("schema");
	    	  arr.add("E:\\shilei\\workspace\\mangocity-data\\mangocity-data-commons\\src\\main\\resources\\lz_user_option_action_log.avsc");
	    	  arr.add("E:\\shilei\\workspace\\mangocity-data\\mangocity-data-commons\\src\\main\\java");
	    	  try {
				tool.run(System.in, System.out, System.err, arr);
			} catch (Exception e) {
				e.printStackTrace();
			}
	      }
	}
	
	public static void main(String[] args) {
		new AppTest().generateAvroJavaFile();
	}
}
