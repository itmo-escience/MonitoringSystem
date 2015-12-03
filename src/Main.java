import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import entities.Task;
import org.bson.Document;
import org.json.*;

import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) {
	// write your code here
        GangliaMonitor gmon = new GangliaMonitor();
        //gmon.GetCurrentMetrics();
        //gmon.GetCurrentMetrics();
        List<String> clusterNames = gmon.GetClusterNames();
        List<String> hostNames = gmon.GetClusterHosts(clusterNames.get(0));

        String[] metrics = new String[]{"cpu_system", "mem_total", "disk_total", "network_total"};
        gmon.GetMetricsByHost(hostNames.get(0), metrics);
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
        String test="1";

    }
}
