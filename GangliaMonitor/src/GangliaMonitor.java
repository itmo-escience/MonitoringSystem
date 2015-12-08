

public class GangliaMonitor {

    public static void main(String[] args) {
        String ApiServerUrl = "localhost";  //"192.168.13.133:8080"
        String[] metrics = new String[]{"cpu_user","mem_total", "part_max_used", "bytes_in", "bytes_out"};

        if(args!=null && args.length>0)
            ApiServerUrl = args[0];

        GangliaAPIMonitor gmon = new GangliaAPIMonitor(ApiServerUrl, metrics);
        gmon.StartMonitoring();
    }

}
