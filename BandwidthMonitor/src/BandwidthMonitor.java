import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.CommonSSHClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class BandwidthMonitor {
    private static Logger log = LogManager.getLogger(BandwidthMonitor.class);
    private CommonMongoClient mongoClient;
    private List<String> hosts;

    public BandwidthMonitor(CommonMongoClient commonMongoClient){
        mongoClient = commonMongoClient;
        hosts = new ArrayList<String>();
        hosts.add("192.168.92.13");
        hosts.add("192.168.92.14");
    }

    public static void main(String[] args){
        BandwidthMonitor bandwidthMonitor = new BandwidthMonitor(new CommonMongoClient());
        bandwidthMonitor.startMonitoring(15*1000);
    }

    public void RunTest(){
        log.info("Running test");
        String sshPath = "d:\\Projects\\MonitoringSystem\\StormMetricsMonitor\\resources\\id_rsa";
        String sshKey = CommonSSHClient.ReadSShKey(sshPath);
        CommonSSHClient client = new CommonSSHClient("192.168.92.11", "vagrant", sshKey);
        String ret = client.executeCommand("iperf -c 192.168.92.14");
        String abc = ret.toString();
    }

    public void startMonitoring(int sleepInterval){
        log.info("Monitoring starting");
        while(1==1){
            try {
                Thread.sleep(sleepInterval);
                log.trace("Sleeping " + sleepInterval+"ms");
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }
}
