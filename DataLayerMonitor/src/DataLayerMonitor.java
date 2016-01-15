import com.mongodb.client.FindIterable;
import itmo.escience.dstorage.utils.Storage;
import itmo.escience.dstorage.utils.enums.StorageLevel;
import itmo.escience.dstorage.utils.obj.ObjectMetaData;
import itmo.escience.dstorage.utils.responses.MetadataResponse;
import itmo.escience.dstorage.utils.responses.Response;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by Pavel Smirnov
 */
public class DataLayerMonitor {
    private CommonMongoClient mongoClient;
    public DataLayerMonitor(CommonMongoClient commonMongoClient){
        mongoClient = commonMongoClient;

    }

    public static void main(String[] args){
        DataLayerMonitor monitor = new DataLayerMonitor(new CommonMongoClient());

        Storage storage = new Storage("192.168.92.11", "8084", "56979ffb0cf24fba120f7a60");
        File file = new File("d:\\Projects\\MonitoringSystem\\Test.txt");
        //Response resp = storage.uploadFile(file, "test.txt", file.length(), 0, "test", 0);
        MetadataResponse metadata0 = (MetadataResponse)storage.getMetaDataByName("test.txt");
        ObjectMetaData obj0 = metadata0.getMetaData().get(0);

        Response move = storage.moveFileLevelByName("test.txt", "192.168.92.11", StorageLevel.SSD, StorageLevel.MEM);

        MetadataResponse metadata1 = (MetadataResponse)storage.getMetaDataByName("test.txt");
        ObjectMetaData obj1 = metadata1.getMetaData().get(0);

        String test="123";
    }

    public void GetChangeLevelRequests(){
        mongoClient.open();
        ArrayList<String> ret = new ArrayList<String>();
        FindIterable<Document> res = mongoClient.getDocumentsFromDB("dstorage.requests", new Document("request", "/lvl/"));
        Iterator keysIter = res.iterator();
        while (keysIter.hasNext()){
            Document resultmap = (Document)keysIter.next();
            ret.add(resultmap.get("started").toString());
        }

        String test="123";
        mongoClient.close();
    }
}
