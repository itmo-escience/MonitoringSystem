import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.JSONObject;

/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {


    private String serverUrl;
    private MongoDatabase defaultDB;

    public CommonMongoClient(){
        MongoClient mongoClient = new MongoClient("192.168.13.133", 27017 );
        defaultDB = mongoClient.getDatabase("logging");
    }

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
}
