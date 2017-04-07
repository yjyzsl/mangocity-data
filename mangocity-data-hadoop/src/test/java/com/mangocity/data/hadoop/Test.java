package com.mangocity.data.hadoop;

import org.apache.avro.mapred.AvroValue;

/**
 *
 * @author shilei
 * @date 2016年6月30日 下午2:37:23 
 */
public class Test {
	
	public static void main(String[] args) {
		B b = new B();
		b.setName("xiaoming");
		b.setTitle("123213");
		b.setAge(12);
		C<B> c = new C<>();
		c.setT(b);
		c.test();
		System.out.println(c.avroValue.datum());
		
	}
	
	static class C<T>{
		private T t;
		AvroValue<T> avroValue = new AvroValue<>();
		
		public T getT() {
			return t;
		}

		public void setT(T t) {
			this.t = t;
		}
		
		public void test(){
			A a = null;
			if(t instanceof B){
				a = (B)t;
			}
			a.setTitle("aaaaa");
			avroValue.datum(t);
		}
		

		@Override
		public String toString() {
			return "C [t=" + t + "]";
		}
		
	}
	
	static class A{
		private String name;
		private  String title;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		@Override
		public String toString() {
			return "A [name=" + name + ", title=" + title + "]";
		}
		
	}
	
	static class B extends A{
		private int age;

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public String toString() {
			return "B [age=" + age + "]"+super.toString();
		}
		
	}

}

