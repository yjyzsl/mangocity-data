package com.mangocity.data.metaq;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.time.DateFormatUtils;

import com.mangocity.data.commons.util.PropertiesUtil;

/**
 * Unit test for simple App.
 */
public class AppTest
{
	private ThreadPoolExecutor taskRunner;
	
	private Thread dataHandler;
	
	private int takeSizeWait;
	
	private final AtomicInteger activeThreadNum = new AtomicInteger(0);
	
	/** Lock held by put, offer, etc */
    private final ReentrantLock putLock = new ReentrantLock();

    /** Wait queue for waiting puts */
    private final Condition notFull = putLock.newCondition();
    
    private final Object lock = new Object();
	
	// 处理线程数量
	private int handlerThreadNum = 4;
	
	public AppTest(){
		takeSizeWait = PropertiesUtil.getNumberValue(MetaqConstants.TAKE_SIZE_WAIT,MetaqConstants.TAKE_SIZE_WAIT_VALUE);
	}
	
	public static void main(String[] args) {
		try {
			new AppTest().start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 启动处理线程
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:07:50
	 * @throws Exception
	 */
	public void start()throws Exception {
		taskRunner = (ThreadPoolExecutor)Executors.newFixedThreadPool(handlerThreadNum);
		dataHandler = new Thread(new DataHandler(this));
		// 启动处理线程
		dataHandler.start();
//		dataHandler = new Thread(new DataHandler2());
//		// 启动处理线程
//		dataHandler.start();
	}
	
	
	private class DataHandler implements Runnable{

		private AppTest appTest;
		
		public DataHandler(AppTest appTest) {
			super();
			this.appTest = appTest;
		}
		
		@Override
		public void run() {
			
			Long minuteTime = System.currentTimeMillis();
			//时间间隔
			Long interval = 0L;
			//每次写文件的时间间隔
			int writerFileInterval = 2*1000;
			int threadNum = -1;
			
			while(true){
				try{
					Long currentTime = System.currentTimeMillis();
					interval = currentTime - minuteTime;
					threadNum = taskRunner.getActiveCount();
					if(threadNum >= handlerThreadNum){
						Thread.sleep(100);
						continue;
					}
					System.out.println("==============================================threadNum:"+threadNum);
					// 从队列中取出一条json数据
					if(interval >= writerFileInterval){//达到了写数据的时间间隔或队列指定容量
					
						// 创建CosumerData对象  minuteTime 12:10 currentTime 12:12 14:10
						taskRunner.submit(new Task(appTest));
						threadNum = activeThreadNum.getAndIncrement();
						System.out.println("DataHandler:"+DateFormatUtils.format(System.currentTimeMillis(), MetaqConstants.DATE_FORMAT_PATTERN_17)+",threadNum:"+threadNum);
						
						// 设置到下一个文件的时间
						minuteTime = currentTime;
						System.out.println("======minuteTime:"+minuteTime);
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	class Task implements Runnable{

		private AppTest appTest;
		
		public Task(AppTest appTest) {
			super();
			this.appTest = appTest;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(13*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			String threadName = Thread.currentThread().getName();
			System.out.println(threadName+"Task:"+DateFormatUtils.format(System.currentTimeMillis(), MetaqConstants.DATE_FORMAT_PATTERN_17));
			System.out.println(taskRunner);
		}
		
	}
}
