import ifmo.escience.dapris.monitoring.common.CommonMongoClient;

/**
 * Created by Pavel Smirnov
 */
public class TestMongoClient {

    public static void main(String[] args){
        CommonMongoClient client = new CommonMongoClient();
        client.open();
        System.out.print("Test successed");

    }
}
