import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONObject;

import java.util.Iterator;

/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {

    public static String metricsCollection = "metrics";
    public static String stateCollection = "clusterStates";
    private String serverUrl;
    private MongoDatabase defaultDB;

    public CommonMongoClient(){
        MongoClient mongoClient = new MongoClient("192.168.13.133", 27017 );
        defaultDB = mongoClient.getDatabase("logging");
    }

    public MongoDatabase getDefaultDB(){ return defaultDB; }

    public CommonMongoClient(String serverUrl, String defaultDBname){
        MongoClient mongoClient = new MongoClient(serverUrl, 27017 );
        defaultDB = mongoClient.getDatabase(defaultDBname);
    }

    public void saveObjectToDB(Object obj, String collection){
        JSONObject stateJSON = new JSONObject(obj);
        String jsonStr = stateJSON.toString();
        Document doc = Document.parse(jsonStr);
        saveDocumentToDB(doc, collection);
    }

    public void saveDocumentToDB(Document doc, String collection){
        MongoCollection coll = defaultDB.getCollection(collection);
        coll.insertOne(doc);
    }

    public FindIterable<Document> getDocumentsFromDB(Document condition, String collection){

         /* Example:
        FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document(), "clusterStates");
        Iterator keysIter = res.iterator();
        while (keysIter.hasNext()){
            //System.out.println(flavoursIter.next());
            Document resultmap = (Document)keysIter.next();
            ret.add(resultmap.get("started").toString());
        }
       */

        MongoCollection coll = defaultDB.getCollection(collection);
        FindIterable<Document> ret = coll.find(condition).projection(new Document("_id", 0));
        return ret;
    }
}
