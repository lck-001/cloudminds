package app;

//import bean.VidioInfo;
//import com.alibaba.fastjson.JSONObject;
//import com.mongodb.hadoop.MongoInputFormat;
//import com.mongodb.hadoop.MongoOutputFormat;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
//import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.hadoop.mapreduce.Job;
//import org.bson.BSONObject;

/**
 * @author maokeluo
 * @date Created in 下午6:53 18-11-27
 */
public class MongoSourceJob {

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        Job inputJob = Job.getInstance();
//        inputJob.getConfiguration().set("mongo.input.uri", "mongodb://172.16.31.1:31061/rod.storage_file_video_2020");
//        HadoopInputFormat<Object, BSONObject> hdIf =
//                new HadoopInputFormat<>(new MongoInputFormat(), Object.class, BSONObject.class, inputJob);
//
//        DataSet<Tuple2<Object, BSONObject>> inputNew = env.createInput(hdIf);
//
//        inputNew.map(tuple2->{
//            VidioInfo vidioInfo = JSONObject.parseObject(tuple2.f1.toString(), VidioInfo.class);
//            return vidioInfo;
//        }).print();
//
////        inputNew.print();
////        DataSet<Tuple2<String, BSONWritable>> personInfoDataSet = inputNew
////                .map(new BSONMapToRecord())
////                .groupBy(new RecordSeclectId())
////                .reduceGroup(new KeyedGroupReduce());
//
//        Job outputJob = Job.getInstance();
//        outputJob.getConfiguration().set("mongo.output.uri", "mongodb://mongo:27017/db.collection");
//        outputJob.getConfiguration().set("mongo.output.batch.size", "8");
//        outputJob.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
//        inputNew.output(new HadoopOutputFormat<>(new MongoOutputFormat<>(), outputJob));
//
//
//        env.execute(MongoSourceJob.class.getCanonicalName());

    }
}
