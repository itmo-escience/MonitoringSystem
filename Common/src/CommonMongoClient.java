package ifmo.escience.dapris.monitoring.common;
import ifmo.escience.dapris.monitoring.common.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.bson.Document;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {
    private static org.apache.logging.log4j.Logger log = LogManager.getLogger(CommonMongoClient.class);
    //private static Logger log = LoggerFactory.getLogger(CommonMongoClient.class);
    public static String metricsCollection = "metrics";
    public static String stateCollection = "clusterStates";

    private String defaultDBname = "logging";
    public MongoClient mongoClient;
    private MongoDatabase defaultDB;
    private Mongo mongo;
    private String host = "192.168.13.133";
    private String username;
    private String password;
    private String configFileName = "Mongo.conf";
    private DB db;
    private Boolean isOpened=false;
    private Boolean closeAtFinish=false;

    public CommonMongoClient(){
        ReadConfigFile();
        //Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        //mongoLogger.setLevel(Level.OFF);

    }

    private void ReadConfigFile(){

//        Configurations configs = new Configurations();
//        try
//        {
//            Configuration config = configs.properties(new File(configFileName));
//            host = config.getString("host");
//            username = config.getString("username");
//            password = config.getString("password");
//            defaultDBname = config.getString("dbname");
//
//
//        }
//        catch (ConfigurationException cex)
//        {
//            log.info("Cannot read external config. Using default values");
//        }

    }

    public MongoDatabase getDefaultDB(){ return defaultDB; }

    public CommonMongoClient(String serverUrl, String defaultDBname){
        this.host = serverUrl;
        this.defaultDBname = defaultDBname;

    }

    public void open(){
        if(isOpened)return;
        log.trace("Mongo client open()");

        if(username!=null && password!=null && !username.equals("") && !password.equals("")){
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(
                    MongoCredential.createMongoCRCredential(username, defaultDBname,password.toCharArray())
            );
            mongoClient = new MongoClient(new ServerAddress(host), credentials);
        }else
            mongoClient = new MongoClient(new ServerAddress(host));


        defaultDB = mongoClient.getDatabase(defaultDBname);

        mongo = new Mongo(host);
        db = mongo.getDB(defaultDBname);
        isOpened=true;
    }

    public void close(){
        //log.trace("Mongo client close()");
        mongoClient.close();
        mongo.close();
        isOpened=false;
        closeAtFinish=false;
    }
//    public void saveObjectToDB(Object obj, String collection){
//        JSONObject stateJSON = new JSONObject(obj);
//        String jsonStr = stateJSON.toString();
//        Document doc = Document.parse(jsonStr);
//        insertDocumentToDB(doc, collection);
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
                log.error(ifmo.escience.dapris.monitoring.common.Utils.GetTracedError(e));
            }
            catch (Exception e) {
                log.error(ifmo.escience.dapris.monitoring.common.Utils.GetTracedError(e));
            }
        }
        obj = JSON.parse((String)obj);
        return (DBObject)obj;
    }

    public String insertDocumentToDB(String collection, Document doc){
        String ret = "";
        if(!isOpened){ open(); closeAtFinish = true;}
        //log.trace("Inserting document to DB: "+collection);
        try {
            MongoCollection coll = defaultDB.getCollection(collection);
            coll.insertOne(doc);
            ret = getDocumentFromDB(collection, doc).get("_id").toString();
        }
        catch (Exception e){
            log.error(Utils.GetTracedError(e));
        }
        if(closeAtFinish)close();
        return ret;
    }

    public void updateDocumentInDB(String collection, Document find, Document update){
        if(!isOpened){ open(); closeAtFinish = true;}
        MongoCollection coll = defaultDB.getCollection(collection);
        try {
            //coll.findOneAndUpdate(find, update);
            coll.findOneAndReplace(find, update);
        }
        catch (Exception e){
            log.error(ifmo.escience.dapris.monitoring.common.Utils.GetTracedError(e));;
        }
        if(closeAtFinish)close();
    }

    public void saveObjectToDB(String collection, Object insert){
        saveObjectToDB(collection, null, insert);
    }


    public void saveObjectToDB(String collection, Object find, Object update){

        if(!(find instanceof DBObject))
            find = CreateDBObject(find);

        if(!(update instanceof DBObject))
            update = CreateDBObject(update);
        if(!isOpened){ open(); closeAtFinish = true;}
        DBCollection dbCollection = db.getCollection(collection);
        try{
            if (find!=null)
                dbCollection.update((DBObject) find, (DBObject) update, true, false);
            else
                dbCollection.save((DBObject)update);
        }
        catch (Exception e){
            log.error(GetTracedError(e));
        }
        if(closeAtFinish)close();
    }

    public Document getDocumentFromDB(String collection, Document condition){
        List<Document> list = getDocumentsFromDB(collection, condition);
        if(list.size()>0)
            return list.get(0);
        return null;
    }

    public List<Document> getDocumentsFromDB(String collection, Document condition){
        return getDocumentsFromDB(collection, condition, null, 0);
    }

    public List<Document> getDocumentsFromDB(String collection, Document condition, Document sort, int limit){
        if(condition==null)
            condition=new Document();

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

//        if (projection==null)
//            projection = new Document("_id", 0);

        if(!isOpened){ open(); closeAtFinish = true;}
        ArrayList<Document> ret = null;
        try {
            MongoCollection coll = defaultDB.getCollection(collection);
            FindIterable<Document> res = coll.find(condition).sort(sort).limit(limit);
            Iterator keysIter = res.iterator();
            ret = new ArrayList<Document>();
            while (keysIter.hasNext()){
                Document doc = (Document)keysIter.next();
                ret.add(new Document(doc));
            }
        }
        catch (Exception ex){
            throw ex;
            //log.error(ex);
        }
        if(closeAtFinish)close();
        return ret;
    }

    public <T> List<T> getObjectsFromDB(String collection, DBObject condition, int limit, Class<T> targetClass){
        //Example:  List<ClusterState> res = getObjectsFromDB("clusterStates", new BasicDBObject(){{ put("name", "Mesos"); }}, ClusterState.class);
        List<T> ret = new ArrayList<T>();
        if(!isOpened){ open(); closeAtFinish = true;}
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
                //log.error(ifmo.escience.dapris.monitoring.common.Utils.GetTracedError(e));;
            }

        }
        if(closeAtFinish)close();
        return ret;
    }

    private DBCursor getObjectsFromDB(String collection, DBObject condition, int limit){

        DBCollection dbCollection = db.getCollection(collection);
        DBCursor cursor = dbCollection.find(condition, new BasicDBObject("_id", 0)).limit(limit);

        return cursor;
    }

    public static String GetTracedError(Exception e){
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }

}


