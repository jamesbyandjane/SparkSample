package jamesby.sparksample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import scala.Tuple2;



public class SampleTask implements java.io.Serializable{

	public static String _FILE_NAME = "hdfs://hdfs1:9000/spark/learning/sample_data.json";
	
	public static String _SPLIT_STRING = "=====";
	
	private static final long serialVersionUID = 1L;

	private JavaSparkContext getJavaSparkContext(){
		SparkConf conf = new SparkConf().setAppName("SampleMain").setMaster("yarn");
		JavaSparkContext sc = new JavaSparkContext(conf);	
		return sc;
	}	
	
	public List<UserKeyObject> compulate() {
	
		
		JavaSparkContext sc = getJavaSparkContext();
		JavaRDD<String> lines = sc.textFile(_FILE_NAME);
		JavaRDD<User> userList1 = lines.map(x->JSON.parseObject(x, new TypeReference<User>() {}));
				

		
		JavaRDD<User> userList2 = userList1.filter(x->!"None".equals(x.getFriends()));
				

		
		JavaRDD<Tuple2<String,String>> userList3 = userList2.flatMap(user->
		{
						
				List<Tuple2<String,String>> result = new ArrayList<>();
				
				Set<String> set = new TreeSet<String>();
				
				if(user.getFriends().indexOf(",")==-1) {
					set.add(user.getFriends());
				}else {
					String[] friends = user.getFriends().split(",");
					for(String item:friends) {
						set.add(item);
					}
				}

				String[] userIdArray = new String[set.size()];
				set.toArray(userIdArray);
				
				for(int i=1;i<=userIdArray.length;i++) {
					List<Tuple2<String,String>> list = new ArrayList<>();
					
					generateFriend(
							user,
							list,
							userIdArray,
							new TreeSet<String>(),
							i,
							0,
							i);			
					
					
					result.addAll(list);
					
				}
				
				
				return result.iterator();
		
		});
		
		JavaPairRDD<String, String> userList4 = userList3.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String, String>(t._1,t._2);
			}
			
		});
				
		
		JavaPairRDD<String,Iterable<String>> userList5=userList4.groupByKey();
		
		JavaRDD<UserKeyObject> userList6 = userList5.map(new Function<Tuple2<String,Iterable<String>>,UserKeyObject>(){

			private static final long serialVersionUID = 1L;

			@Override
			public UserKeyObject call(Tuple2<String, Iterable<String>> v1) throws Exception {
				UserKeyObject obj = new UserKeyObject();
				obj.setFriends(v1._1);
				
				Iterator<String> iterator =  v1._2.iterator();
				String userNames = "";
				int count = 0;
				while (iterator.hasNext()) {
					userNames = userNames + ","+iterator.next();
					count ++;
				}
				obj.setUsernames(userNames.substring(1));
				obj.setCount(count);
				return obj;
			}
			
		});


		
		JavaRDD<UserKeyObject> userList7 = userList6.filter(x->{
			int friends = x.getFriends().split(",").length;
			int count = x.getCount();
			if (count==friends)
				return true;
			return false;
		});
		
		List<UserKeyObject> userListResult = userList7.collect();
		return userListResult;
	}

	public static class UserKeyObject implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private String friends;
		private String usernames;
		private int count;
		public String getFriends() {
			return friends;
		}
		public void setFriends(String friends) {
			this.friends = friends;
		}
		public String getUsernames() {
			return usernames;
		}
		public void setUsernames(String usernames) {
			this.usernames = usernames;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		
	}
	
	public static void generateFriend(
			User currentUser,
			List<Tuple2<String,String>> result,
			String[] userIdArray,
			Set<String> currentSet,
			int friendIndex,
			int pos,
			int friends){
		
		if (friendIndex==0)
			return;
		
		Set<String> cloneSetA = new TreeSet<String>();
		
		cloneSetA.addAll(currentSet);
		
		for(int i=pos;i<userIdArray.length;i++) {
			cloneSetA.add(userIdArray[i]);
			generateFriend(currentUser,result,userIdArray,cloneSetA,friendIndex-1,pos+1,friends);			
			if (cloneSetA.size()==friends) {
				result.add(getString(currentUser,cloneSetA));
			}			
			cloneSetA.remove(userIdArray[i]);
		}
		
	}
	
	
	public static Tuple2<String,String> getString(User currentUser,Set<String> set) {

		Set<String> cloneSetA = new TreeSet<String>();
		
		cloneSetA.addAll(set);
		cloneSetA.add(currentUser.getUser_id());
		
		StringBuffer buf = new StringBuffer("");
		for(String str:cloneSetA) {
			buf.append(","+str);
		}
		
		return new Tuple2<String,String>(buf.toString().substring(1),currentUser.getName());
	}
	
	public static void main(String[] args) {
		List<Tuple2<String,String>> result = new ArrayList<>();
		String[] userIdArray = new String[] {"1","2","4","5"};
		
		User u = new User();
		u.setName("A");
		u.setUser_id("3");
		
		for(int i=1;i<=userIdArray.length;i++) {
			
			List<Tuple2<String,String>> list = new ArrayList<>();
			
			generateFriend(
					u,
					list,
					userIdArray,
					new TreeSet<String>(),
					i,
					0,
					i);			
//			
			result.addAll(list);
			
		}
		
		for(Tuple2<String,String> item:result) {
			System.out.println(item._1+" "+item._2+" ----------------------");

		}
	}
}
