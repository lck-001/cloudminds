package app;

import com.mongodb.BasicDBObject;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class MongoDriverApp {
    /**
     * 查询打印全部集合
     */
    public static void mongoQueryAll() {
        //1.创建链接
        MongoClient client = new MongoClient("172.16.31.1:31061");
        //2.打开数据库test
        MongoDatabase db = client.getDatabase("rod");
        //3.获取集合
        MongoCollection<Document> collection = db.getCollection("storage_file_anbaovideo_2022");
        //4.查询获取文档集合T
        Calendar startCalendar=Calendar.getInstance();
        startCalendar.set(2021, 10, 1,0,0,0);  //年月日  也可以具体到时分秒如calendar.set(2015, 10, 12,11,32,52);
        Date startDate=startCalendar.getTime();//date就是你需要的时间
        Calendar endCalendar=Calendar.getInstance();
        endCalendar.set(2021, 11, 1,0,40,0);
        Date endDate=endCalendar.getTime();//date就是你需要的时间
        HashMap<String, BasicDBObject> dateMap = new HashMap<>();
        dateMap.put("createTime",new BasicDBObject("$gt",startDate));
        dateMap.put("updateTime",new BasicDBObject("$lt",endDate));
        BasicDBObject basicDBObject = new BasicDBObject();
//        BasicDBObject append = basicDBObject.append("createTime",new BasicDBObject("$gt",startDate)).append("updateTime",new BasicDBObject("$lt",endDate));
//        FindIterable<Document> documents = collection.find(new BasicDBObject("updateTime","Mon Jan 04 19:24:10 CST 2021"));
//        FindIterable<Document> documents = collection.find(new BasicDBObject(dateMap)).limit(1);
//        FindIterable<Document> documents = collection.find(new BasicDBObject("createTime",new BasicDBObject("$gt",startDate)).append("createTime",new BasicDBObject("$lt",endDate)));
        FindIterable<Document> documents = collection.find().limit(3);

        System.out.println("集合大小：=="+collection.count());
//        System.out.println("集合半年大小：=="+collection.count(new BasicDBObject("createTime",new BasicDBObject("$gt",startDate)).append("createTime",new BasicDBObject("$lt",endDate))));
//        System.out.println("集合半年大小：=="+collection.count(append));
//        System.out.println("集合半年大小：=="+collection.count(new BasicDBObject(dateMap)));
        //5.循环遍历
        for (Document document : documents) {
            System.out.println(document);
        }
        //6.关闭连接
        client.close();
    }

    public static void main(String[] args) {
        mongoQueryAll();
    }
}
