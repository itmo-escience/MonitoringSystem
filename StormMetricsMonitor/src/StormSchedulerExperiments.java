import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import com.jcabi.ssh.Shell;
import com.jcabi.ssh.SSH;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by Pavel Smirnov
 */
public class StormSchedulerExperiments {
    private static Logger log = LogManager.getLogger(StormSchedulerExperiments.class);
    private CommonMongoClient mongoClient;
    private String sshKey = "";
    String collectionName = "storm.experiments";
    String masterHost = "192.168.92.11";
    String baseUrl = "http://192.168.92.11:8080/api/v1/";
    String[] slaveHosts = new String[]{"192.168.92.12", "192.168.92.13", "192.168.92.14", "192.168.92.15", "192.168.92.16"};
    private HashMap<String, SSH> shell = new HashMap<String, SSH>();
    private HashMap<String, String> schedulers = new HashMap<String, String>();

    public static void main(String[] args){
        StormSchedulerExperiments stormMetricsMonitor = new StormSchedulerExperiments("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.killAll();
        stormMetricsMonitor.startExperiments();
     }

    public StormSchedulerExperiments (String baseUrl){
        sshKey = ReadSShKey();
//        this.baseUrl = baseUrl;
//        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
        schedulers.put("default", "org.apache.storm.scheduler.DefaultScheduler");
        schedulers.put("resource", "org.apache.storm.scheduler.resource.ResourceAwareScheduler");
        schedulers.put("annealing", "org.apache.storm.scheduler.annealing.AnnealingScheduler");
    }

    public void startExperiments(int seconds){
        log.trace("Start experiments");
        try {
            for(String scheduler : schedulers.keySet()){
                for(int megabyte: new int[]{ 1 /*, 5, 10*/ }){
                    Document experiment = new Document();
                    experiment.put("started", new Date());
                    //switchScheduler(scheduler);
                    String startsWith = submitTopology(scheduler, 1024, 5, 1, 3);
                    //String startsWith = "patient_default_5242880_1024_1_3";
                    String topoId = getTopoId(startsWith);
                    experiment.put("topoId", topoId);
                    experiment.put("kbSize", 1024);
                    experiment.put("workers", 5);
                    experiment.put("emitters", 1);
                    experiment.put("processors", 1);
                    log.trace("Sleeping "+String.valueOf(seconds)+" seconds");
                    Thread.sleep(seconds * 1000);
                    experiment.put("finished", new Date());
                    mongoClient.insertDocumentToDB(collectionName, experiment);
                    killTopology(startsWith);
                    killAll();
                 }
            }
        } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    public String getTopoId(String startsWith){
        log.trace("Getting topology ID");
        String topoId=null;
        while(topoId==null){
            try{
                JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");
                for (Object topology : topologies) {
                    String id = ((JSONObject) topology).get("id").toString();
                    //if (id.startsWith(startsWith)) {
                        topoId = id.split("-")[0];
                        break;
                    //}
                }

            }
            catch (Exception e) {
                try{
                    Thread.sleep(5000);
                } catch (InterruptedException e2){

                }
            }
        }
        return topoId;
    }

    public JSONArray WaitForJSON(String url, String property){
        log.trace("Waiting for "+property);
        JSONArray topologies = null;
        while(topologies==null) {
            try{
                JSONObject summary = Utils.getJsonFromUrl(url);
                topologies = (JSONArray) summary.get(property);
            }
            catch (Exception e) {
               try{
                   Thread.sleep(5000);
               } catch (InterruptedException e2){

               }
            }
        }
        return topologies;
    }

    public String ReadSShKey(){
        String ret = "";
        BufferedReader br = null;
        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("d:\\Projects\\MonitoringSystem\\StormMetricsMonitor\\resources\\id_rsa"));
            while ((sCurrentLine = br.readLine()) != null) {
                if(ret!="")ret+="\n";
                ret+=sCurrentLine;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return ret;
    }

    public void executeCommand(String host, String command){
        SSH hostShell=null;
        if(shell.containsKey(host))
            hostShell = shell.get(host);
        if (hostShell==null){
            try {
                hostShell = new SSH(host, 22, "vagrant", sshKey);
                if(!shell.containsKey(hostShell))
                    shell.put(host,hostShell);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        try{
            System.out.print(command+"\n");
            String stdout = new Shell.Plain(hostShell).exec(command);
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    public void switchScheduler(String name){
        String newString = "\""+ schedulers.get(name)+"\"";
        for (String schedName: schedulers.keySet())
            if(!schedName.equals(name)){
                String oldString = "\""+ schedulers.get(schedName)+"\"";
                executeCommand(masterHost, "sudo sed -i \"s/"+oldString+"/"+newString+"/g\" /opt/storm/defaults.yaml");
            }
        executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
        WaitForJSON(baseUrl + "topology/summary", "topologies");
    }

    public String submitTopology(String schedulerName, int kbsize, int workers, int emitters, int processors  ){
        String topoName = "patient_"+schedulerName+" "+workers+" "+kbsize+" "+emitters+" "+processors;
        log.trace("Submitting topology "+topoName);
        executeCommand(masterHost,"/opt/storm/bin/storm jar /opt/storm-examples/seizure-light-2.0.0-SNAPSHOT-jar-with-dependencies.jar seizurelight.SeizurePredictionTopology "+topoName);
        return topoName.replace(" ","_");
    }

    public void killTopology(String topologyName){
        log.trace("Killing topology "+topologyName);
        executeCommand(masterHost,"/opt/storm/bin/storm kill "+topologyName);
    }

    public void killAll(){
        log.trace("Killing unkilled storm processes");
        for (String slaveHost: slaveHosts)
             executeCommand(slaveHost,"sudo killall java");


        Integer topos = 1;
        while(topos>0) {
            try {
                JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");
                topos = topologies.length();
            } catch (Exception e) {
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e2){

                }
            }
        }
    }
}
