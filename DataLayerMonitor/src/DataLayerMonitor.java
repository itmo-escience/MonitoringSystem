import com.mongodb.client.FindIterable;
import itmo.escience.dstorage.utils.Storage;
import itmo.escience.dstorage.utils.enums.StorageLevel;
import itmo.escience.dstorage.utils.obj.ObjectMetaData;
import itmo.escience.dstorage.utils.responses.MetadataResponse;
import itmo.escience.dstorage.utils.responses.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Pavel Smirnov
 */
public class DataLayerMonitor {
    private static Logger log = LogManager.getLogger(DataLayerMonitor.class);
    private String configFileName = "d:\\Projects\\MonitoringSystem\\DataLayerMonitor\\DCStorageMonitor.config.config";
    private Storage storage;
    private String storageHost = "192.168.92.11:8084";
    private String storageSecID = "56979ffb0cf24fba120f7a60";
    private CommonMongoClient mongoClient;

    public DataLayerMonitor(CommonMongoClient commonMongoClient){
        readConfigFile();
        mongoClient = commonMongoClient;
        storage = new Storage(storageHost.split(":")[0],storageHost.split(":")[1], "56979ffb0cf24fba120f7a60");

    }



    public static void main(String[] args){
        DataLayerMonitor dataLayerMonitor = new DataLayerMonitor(new CommonMongoClient());
        //dataLayerMonitor.GetNodeNames();
//        //File file = new File("d:\\Projects\\MonitoringSystem\\Test.txt");
//        File file = new File("d:\\Share\\data.zip");
//        //Response resp1 = storage.uploadFile(file, "data.zip", file.length(), 0, "data.zip", 0);
//        Response resp = storage.downloadFile("data.zip","data.zip", 0);
//        MetadataResponse metadata0 = (MetadataResponse)storage.getMetaDataByName("data.zip");
//        ObjectMetaData obj0 = metadata0.getMetaData().get(0);
//
//        Response move = storage.moveFileLevelByName("test.txt", "192.168.92.11", StorageLevel.SSD, StorageLevel.MEM);
//
//        MetadataResponse metadata1 = (MetadataResponse)storage.getMetaDataByName("test.txt");
//        ObjectMetaData obj1 = metadata1.getMetaData().get(0);
//
//        String test="123";
    }

    private void readConfigFile(){
        try {
            List<String> lines = Utils.ReadConfigFile(configFileName);
            for (int i = 0; i < lines.size(); i++) {
                if (i == 0) storageHost = lines.get(i);
                else
                    storageSecID = lines.get(i);
                i++;
            }
        }
        catch (Exception e){
            log.info("Cannot read external config. Using default values");
        }
    }

//    private void TestOperations(){
//        //File file = new File("d:\\Projects\\MonitoringSystem\\Test.txt");
//        File file = new File("d:\\Projects\\MonitoringSystem\\Dog_1_test_segment_0.hdf5");
//        Response resp1 = storage.uploadFile(file, "Dog_1_test_segment_0.hdf5", file.length(), 0, "Dog_1_test_segment_0.hdf5", 0);
//        //Response resp = storage.downloadFile("data.zip","data.zip", 0);
//        Response resp = storage.downloadFile("Dog_1_test_segment_0.hdf5","Dog_1_test_segment_0.hdf5", 0);
//        GetAgents();
//        GetFileNames();
//        Response move = storage.moveFileLevelByName("test.txt", "192.168.92.11", StorageLevel.SSD, StorageLevel.MEM);
//    }

    public HashMap<String, Object> GetMetadataByObjectID(String objectID){
        HashMap<String, Object> ret = null;
        MetadataResponse metadata = (MetadataResponse)storage.getMetaDataByName(objectID);
        if (metadata.getStatus()){
            ObjectMetaData obj = metadata.getMetaData().get(0);
            List<String> layers = new ArrayList<String>();
            Map<String, List<Long>> agents = obj.getAgents();
            for (String agentName : agents.keySet()){
                for (Long levelID : agents.get(agentName)) {
                    if (levelID == 0) layers.add("HDD@" + agentName.replace("_","."));
                    if (levelID == 1) layers.add("SSD@" + agentName.replace("_","."));
                    if (levelID == 2) layers.add("RAM@" + agentName.replace("_","."));
                }
            }
            ret = new HashMap<String, Object>();
            ret.put("id", obj.getObjID());
            ret.put("name", objectID);
            ret.put("layers", layers);
            ret.put("size", obj.getSize());
        }
        return ret;
    }

    public void GetLayersByAgentID(String nodeID){

        //ObjectMetaData obj = metadata.getMetaData().get(0);

        String test="123";

    }


    public List<String> GetFileNames(){
        List<String> ret = new ArrayList<String>();
        List<HashMap<String, String>> putRequests = GetRequestsFromDB(new Document("request", new Document("$regex",  ".*\"PUT\".*")));
        ret = putRequests.stream().map(f -> f.get("name")).collect(Collectors.toList());
        ret = new ArrayList<String>(new HashSet<>(ret));
        return ret;
    }



    public TreeMap<String, TreeMap<String, String>> GetAgents(){
        //List<String> ret = new ArrayList<String>();
        TreeMap<String, TreeMap<String, String>> ret = new TreeMap<String, TreeMap<String, String>>();
        List<HashMap<String, String>> regRequests = GetRequestsFromDB(new Document("request", new Document("$regex",  ".*\"register\".*")));
        for(int i=0; i< regRequests.size(); i++){
            HashMap<String, String> requestMap = regRequests.get(i);
            if(!ret.containsKey(requestMap.get("ip")))
                ret.put(requestMap.get("ip"), new TreeMap(requestMap));

        }
        return ret;

    }

    public List<HashMap<String, String>> GetChangeLevelRequests(){
        return GetRequestsFromDB(new Document("request", "/lvl/"));
    }

    public List<HashMap<String, String>> GetRequestsFromDB(Document query){
        mongoClient.open();
        ArrayList<HashMap<String, String>> ret = new ArrayList<HashMap<String, String>>();
        List<Document> res = mongoClient.getDocumentsFromDB("dstorage.requests", query);
        Iterator keysIter = res.iterator();
        for(Document resultmap: res){

            //ret.add(resultmap.get("request").toString());

            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse("{"+resultmap.get("request")+"}");
                json.put("time",resultmap.get("time"));
                ret.add(json);
                String test="123";
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        mongoClient.close();
        return ret;
    }


}
