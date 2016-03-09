package ifmo.escience.dapris.monitoring.bandwidthMonitor;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.CommonSSHClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Pavel Smirnov
 */
public class BandwidthMonitor {
    private static Logger log = LogManager.getLogger(BandwidthMonitor.class);
    private CommonMongoClient mongoClient;
    private List<String> hosts = new ArrayList<String>();
    private HashMap<String, CommonSSHClient> sshClients = new HashMap<String, CommonSSHClient>();
    private String sshPath = "d:\\Projects\\MonitoringSystem\\StormMetricsMonitor\\resources\\id_rsa";
    private String sshKey;
    private String collection = "bandwidths";

    public static void main(String[] args){
        int seconds = 15;
        if(args.length>0)
            seconds = Integer.parseInt(args[0]);
        BandwidthMonitor bandwidthMonitor = new BandwidthMonitor(new CommonMongoClient());
        bandwidthMonitor.startMonitoring(seconds*1000);
    }

    public BandwidthMonitor(CommonMongoClient commonMongoClient){
        mongoClient = commonMongoClient;
        initHosts();
        sshKey = CommonSSHClient.ReadSShKey(sshPath);
    }

    public void initHosts(){

        hosts.add("192.168.92.11");
        hosts.add("192.168.92.13");
        hosts.add("192.168.92.14");
        hosts.add("192.168.92.15");
        hosts.add("192.168.92.16");

    }

    public Double[] GetBandwidth(String from, String to){
        log.info("Getting bandwidth between "+from+" and "+to);

        if(!sshClients.containsKey(from))
            sshClients.put(from, new CommonSSHClient(from, "vagrant", sshKey));

        CommonSSHClient client = sshClients.get(from);

        String ret = client.executeCommand("iperf -c "+to);
        String[] splitted = ret.split("\n");
        String lastLine = splitted[splitted.length-1];
        //String lastLine = "[  3]  0.0-10.0 sec   936 MBytes   785 Mbits/sec";
        Pattern p = Pattern.compile("sec[ ]+(?<bytes>[^ ]+) (?<bytesGrade>[^Bytes]+)Bytes[ ]+(?<bits>[^ ]+) (?<bitsGrade>[^bits]+)");
        Matcher m = p.matcher(lastLine);
        m.find();

        try {
            Double transferredGB = Double.parseDouble(m.group("bytes"));
            if(m.group("bytesGrade").equals("M"))
                transferredGB *= 0.001;
            Double bandwidthGB = Double.parseDouble(m.group("bits"));
            if(m.group("bitsGrade").equals("M"))
                bandwidthGB *= 0.001;
            return new Double[]{ transferredGB, bandwidthGB };
        }
        catch(Exception e){
            log.error("Cannot parse string: "+ lastLine);
        }

        return null;
    }


    public void startMonitoring(int sleepInterval){
        log.info("Monitoring starting");
        int i=0;
        while(true){
            String from = hosts.get(i);
            for(int j=i+1; j<hosts.size(); j++){
                String to = hosts.get(j);
                if(!to.equals(from)){
                    log.info(from+" to "+to);
                    Double[] bandwidthStat = GetBandwidth(from, to);
                    if(bandwidthStat!=null){
                        org.bson.Document ins = new org.bson.Document();
                        ins.put("from", from);
                        ins.put("to", to);
                        ins.put("transferredGB", bandwidthStat[0]);
                        ins.put("bandwidthGB", bandwidthStat[1]);
                        ins.put("time", new Date());
                        mongoClient.insertDocumentToDB(collection, ins);
                    }
                }
            }
            i++;
            if(i==hosts.size()) {
                i = 0;
                log.trace("Sleeping " + sleepInterval+"ms");
                Utils.Wait(sleepInterval);
            }
        }

    }
}
