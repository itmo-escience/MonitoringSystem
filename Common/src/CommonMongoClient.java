import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.*;
import com.mongodb.bulk.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {
    private static Log log = LogFactory.getLog(CommonMongoClient.class);
    public static String metricsCollection = "metrics";
    public static String stateCollection = "clusterStates";
    private String serverUrl = "192.168.13.133";
    private String defaultDBname = "logging";
    private MongoDatabase defaultDB;
    private DB db;

    public CommonMongoClient(){
        Init();
    }

    public MongoDatabase getDefaultDB(){ return defaultDB; }

    public CommonMongoClient(String serverUrl, String defaultDBname){
        this.serverUrl = serverUrl;
        this.defaultDBname = defaultDBname;
        Init();
    }

    public void Init(){
        MongoClient mongoClient = new MongoClient(serverUrl, 27017 );
        Mongo mongo = new Mongo(serverUrl, 27017);
        defaultDB = mongoClient.getDatabase(defaultDBname);
        db = mongo.getDB(defaultDBname);
    }
//    public void saveObjectToDB(Object obj, String collection){
//        JSONObject stateJSON = new JSONObject(obj);
//        String jsonStr = stateJSON.toString();
//        Document doc = Document.parse(jsonStr);
//        saveDocumentToDB(doc, collection);
//    }
    public DBObject CreateDBObject(Object obj){
        if(!(obj instanceof String)){
            ObjectMapper mapper = new ObjectMapper();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm a z");
            mapper.setDateFormat(df);
            try {
                mapper.writeValue(new File("tmp.json"), obj);
                obj = mapper.writeValueAsString(obj);
            } catch (IOException e){
                log.error(e);
            }
            catch (Exception e) {
                log.error(e);
            }
        }
        obj = JSON.parse((String)obj);
        return (DBObject)obj;
    }

    public void saveDocumentToDB(String collection, Document doc){
        MongoCollection coll = defaultDB.getCollection(collection);
        coll.insertOne(doc);
        return;

    }

    public void saveObjectToDB(String collection, Object insert){
        saveObjectToDB(collection, null, insert);
    }



    public void saveObjectToDB(String collection, Object find, Object update){

        if(!(find instanceof DBObject))
            find = CreateDBObject(find);

        if(!(update instanceof DBObject))
            update = CreateDBObject(update);
        DBCollection dbCollection = db.getCollection(collection);
        if (find!=null)
            dbCollection.update((DBObject) find, (DBObject) update, true, false);
        else
            dbCollection.save((DBObject)update);
    }

    public FindIterable<Document> getDocumentsFromDB(String collection, Document condition){
        return  getDocumentsFromDB(collection, condition, null, 0);
    }

    public FindIterable<Document> getDocumentsFromDB(String collection, Document condition, Document sort, int limit){
        if(sort==null)
            sort = new Document("_id",1);
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
        FindIterable<Document> ret = coll.find(condition).sort(sort).projection(new Document("_id", 0)).limit(limit);
        return ret;
    }

    public <T> List<T> getObjectsFromDB(String collection, DBObject condition, int limit, Class<T> targetClass){
        //Example:  List<ClusterState> res = getObjectsFromDB("clusterStates", new BasicDBObject(){{ put("name", "Mesos"); }}, ClusterState.class);
        List<T> ret = new ArrayList<T>();

        DBCursor cursor = getObjectsFromDB(collection, condition, limit);
        while (cursor.hasNext()){
            BasicDBObject dbObj = (BasicDBObject)cursor.next();
            String jsonStr = dbObj.toJson().toString();
            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            T retObj = null;
            try {
                retObj = mapper.readValue(jsonStr, targetClass);
                ret.add(retObj);
            } catch (IOException e) {
                log.error(e);
            }

        }
        return ret;
    }

    public DBCursor getObjectsFromDB(String collection, DBObject condition, int limit){

        DBCollection dbCollection = db.getCollection(collection);
        DBCursor cursor = dbCollection.find(condition, new BasicDBObject("_id", 0)).limit(limit);

        return cursor;
    }
}


