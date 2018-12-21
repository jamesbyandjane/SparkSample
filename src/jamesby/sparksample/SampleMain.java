package jamesby.sparksample;

import java.util.List;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import jamesby.sparksample.SampleTask.UserKeyObject;

public class SampleMain {
	private static Logger logger = Logger.getLogger(SampleMain.class);
	
	
	public void compulate() {

	}
	
	public static void main(String[] args) {
		String json = "{\"user_id\":\"lzlZwIpuSWXEnNS91wxjHw\",\"name\":\"Susan\",\"review_count\":1,\"yelping_since\":\"2015-09-28\",\"friends\":\"None\",\"useful\":0,\"funny\":0,\"cool\":0,\"fans\":0,\"elite\":\"None\",\"average_stars\":2.0,\"compliment_hot\":0,\"compliment_more\":0,\"compliment_profile\":0,\"compliment_cute\":0,\"compliment_list\":0,\"compliment_note\":0,\"compliment_plain\":0,\"compliment_cool\":0,\"compliment_funny\":0,\"compliment_writer\":0,\"compliment_photos\":0}";
		
		User user = JSON.parseObject(json, new TypeReference<User>() {});
		
		List<UserKeyObject> list = new SampleTask().compulate();
		logger.info("****************************************************");
		list.forEach(x->{
			logger.info("----------------"  +x.getUsernames()+","+x.getFriends()+","+x.getCount()+"-----------------");
			
		});
		logger.info("****************************************************");

	}
}
