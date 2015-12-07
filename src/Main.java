import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import entities.Task;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class Main {


    public static void main(String[] args) {
	// write your code here


        //GetMesosAllocations();
        //GetCurrentMetrics();

    }

//    public static void GetCurrentMetrics(){
//        String[] metrics = new String[]{"cpu_user","mem_total", "part_max_used", "bytes_in", "bytes_out"};
//        MongoClient mongoClient = new MongoClient( "192.168.13.133" , 27017 );
//        MongoDatabase db = mongoClient.getDatabase("logging");
//        MongoCollection coll = db.getCollection("metrics");
//
//        GangliaAPIMonitor gmon = new GangliaAPIMonitor("localhost"); //"192.168.13.133:8080"
//        List<String> clusterNames = gmon.GetClusterNames();
//        while(1==1) {
//            try {
//                for (String clusterName : clusterNames) {
//                    List<String> hostNames = gmon.GetClusterHosts(clusterName);
//                    for (String hostName : hostNames){
//                        HashMap<String, String> hostMetrics = gmon.GetMetricsByHost(hostName, metrics);
//                        if(hostMetrics.keySet().size()>0){
//                            Document doc = new Document("host", hostName);
//                            doc.append("time", new Date());
//                            for (String key : hostMetrics.keySet())
//                                doc.append(key, hostMetrics.get(key));
//                            db.getCollection("metrics").insertOne(doc);
//                        }
//                    }
//                }
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    public static void GetMesosAllocations(){
        MongoClient mongoClient = new MongoClient( "192.168.13.133" , 27017 );
        MongoDatabase db = mongoClient.getDatabase("logging");
        MongoCollection coll = db.getCollection("mesos");

        //BasicDBObject query = new BasicDBObject("message", new BasicDBObject("$in","Recovered/i"));
        String pattern0 = "(?<time>[^ ]+ [^ ]+)[ ]+(?<threadID>[0-9]+)[ ]+(?<source>[^]]+)] [Recovered cpus(*):]+(?<recoveredCPU>[^;]+); [mem(*):]+(?<recoveredRAM>[^;]+); [disk(*):]+(?<recoveredHDD>[^;]+); [ports(*):]+[[](?<recoveredPorts>[^]]+)] [total: cpus(*):]+(?<totalCPU>[^;]+); [mem(*):]+(?<totalRAM>[^;]+); [disk(*):]+(?<totalHDD>[^;]+); [ports(*):]+[[](?<totalports>[^]]+)], [allocated: cpus(*):]+(?<allocatedCPU>[^;]+); [mem(*):]+(?<allocatedRAM>[^)]+)[) on slave ]+(?<slave>[^ ]+)[ from framework ]+(?<framework>[^ ]+)";
        Pattern pattern = Pattern.compile(pattern0);
        BasicDBObject query = new BasicDBObject("source", "/hierarchical.hpp:1103/");
        //BasicDBObject query = new BasicDBObject("message", "/Recovered/");
        MongoCursor cursor = coll.find(query).iterator();

        List<Task> tasks = new ArrayList<Task>();
        try {
            while(cursor.hasNext()){
                Object item = cursor.next();
                //Task task = new Task(item);
                String test = "";
            }
        } finally {
            cursor.close();
        }
        String test="1";

    }

    public void GetClavireTasks(){
        MongoClient mongoClient = new MongoClient( "192.168.13.133" , 27017 );
        MongoDatabase db = mongoClient.getDatabase("logging");
        MongoCollection coll = db.getCollection("clavire_overhs");
        BasicDBObject query = new BasicDBObject("sort", "565eea4747d0e435ce0146b5");

        MongoCursor cursor = coll.find().iterator();

        //System.out.println(myDoc.forEach(););
        List<Task> tasks = new ArrayList<Task>();
        try {
            while(cursor.hasNext()){
                Object item = cursor.next();
                Task task = new Task(item);
                tasks.add(task);
            }
        } finally {
            cursor.close();
        }


    }

}
