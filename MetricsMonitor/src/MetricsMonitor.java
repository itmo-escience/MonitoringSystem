
/**
 * Created by Pavel Smirnov
 */
public class MetricsMonitor {

    public static void main(String[] args) {
        String ApiServerUrl = "localhost";  //"192.168.13.133:8080"
        String[] metrics = new String[]{ "bytes_in", "bytes_out", "cpu_user","mem_free", "part_max_used"};

        if(args!=null && args.length>0)
            ApiServerUrl = args[0];

        GangliaAPIMonitor gmon = new GangliaAPIMonitor(ApiServerUrl, metrics);
        gmon.mongoClient = new CommonMongoClient();
        gmon.StartMonitoring();
    }

}
