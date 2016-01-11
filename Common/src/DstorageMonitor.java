import com.mongodb.client.FindIterable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by Pavel Smirnov
 */
public class DstorageMonitor {
    private CommonMongoClient mongoClient;
    public DstorageMonitor(CommonMongoClient commonMongoClient){
        mongoClient = commonMongoClient;

    }

    public void GetLeveledRequests(){
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
