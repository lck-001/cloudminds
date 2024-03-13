package utils;

import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MongoDBSource extends RichParallelSourceFunction<String> {
    private MongoClient client;
    //队列允许存放数据size
    private BlockingQueue<String> queue = new ArrayBlockingQueue<String>(2000);
    private List<String> resultSet = new ArrayList<>();


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        connect();
        query();
        while (queue.iterator().hasNext()) {
            sourceContext.collect(queue.take());
        }
    }

    @Override
    public void cancel() {
        client.close();
    }

    //包装连接的方法
    private void connect() {
        try {
            client = new MongoClient("172.16.31.1:31061");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void query() throws InterruptedException {
        //1.打开数据库test
        MongoDatabase db = client.getDatabase("rod");
        //2.获取集合
        MongoCollection<Document> collection = db.getCollection("storage_file_video_2020");
        //3.查询获取文档集合
        Calendar startCalendar=Calendar.getInstance();
        startCalendar.set(2021, 11, 1,0,0,0);  //年月日  也可以具体到时分秒如calendar.set(2015, 10, 12,11,32,52);
        Date startDate=startCalendar.getTime();//date就是你需要的时间
        Calendar endCalendar=Calendar.getInstance();
        endCalendar.set(2021, 12, 1,0,0,0);
        Date endDate=endCalendar.getTime();//date就是你需要的时间
        HashMap<String, BasicDBObject> dateMap = new HashMap<>();
        dateMap.put("createTime",new BasicDBObject("$gt",startDate));
        dateMap.put("updateTime",new BasicDBObject("$lt",endDate));

        FindIterable<Document> documents = collection.find();
        System.out.println("集合总数===" + collection.count());

        for (Document document : documents) {
            queue.put(JSON.toJSONString(document));
        }
    }
}
