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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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
    String[] slaveHosts = new String[]{"192.168.92.13", "192.168.92.14", "192.168.92.15", "192.168.92.16"};
    private HashMap<String, Shell.Plain> shellPlains = new HashMap<String, Shell.Plain>();
    private HashMap<String, String> schedulers = new HashMap<String, String>();

    public static void main(String[] args){
        StormSchedulerExperiments stormMetricsMonitor = new StormSchedulerExperiments("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.killAllProcesses();
        stormMetricsMonitor.startExperiments(60);
     }

    public StormSchedulerExperiments (String baseUrl){
        sshKey = ReadSShKey();
//        this.baseUrl = baseUrl;
//        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
        schedulers.put("default", "org.apache.storm.scheduler.annealing.DefaultSchedulerEmulator");
        schedulers.put("resource", "org.apache.storm.scheduler.resource.ResourceAwareScheduler");
        schedulers.put("annealing", "org.apache.storm.scheduler.annealing.AnnealingScheduler");
    }

    public void startExperiments(int seconds){

        killTopologies();
        killAllProcesses();
        log.info("Starting experiments");

        for(int megabyte: new int[]{ 1 /*, 5, 10*/ }){
            String startsWith = submitTopology(1024, 5, 1, 3);
            Document experiment = new Document();
            experiment.put("started", new Date());
            //String startsWith = "patient_default_5242880_1024_1_3";
            String topoId = getTopoId(startsWith);
            Document find = new Document(){{ put("topoId", topoId); }};
            experiment.put("topoId", topoId);
            experiment.put("kbSize", 1024);
            experiment.put("workers", 5);
            experiment.put("emitters", 1);
            experiment.put("processors", 1);
            mongoClient.insertDocumentToDB(collectionName, experiment);

            for(String scheduler : schedulers.keySet()){
                switchScheduler(scheduler);
                experiment.put(scheduler+"Started", new Date());
                log.info("Doing experiment with "+" scheduler ("+String.valueOf(seconds)+") seconds");
                mongoClient.updateDocumentInDB(collectionName, find, experiment);
                Wait(seconds * 1000);
            }
            experiment.put("finished", new Date());
            mongoClient.updateDocumentInDB(collectionName, find, experiment);
            killTopology(startsWith);
            killAllProcesses();
        }

    }


    public String getTopoId(String startsWith){
        log.info("Waiting for submitted topology ID");
        String topoId=null;
        Integer wait = 5000;
        while(topoId==null){
            try{

                List<String> topoIDs = GetTopologyIDs();
                for (String id : topoIDs){
                        topoId = id.split("-")[0];
                        wait=1;
                        break;
                }

            }
            catch (Exception e){

            }
            Wait(wait);
        }
        return topoId;
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
        log.trace(host+" -> "+command);

        Shell.Plain shellPlain=null;
        if(shellPlains.containsKey(host))
            shellPlain = shellPlains.get(host);

        log.trace("Waiting for ssh conection to "+host);
        Integer wait = 2000;
        while (shellPlain==null){
            try {
                SSH hostShell = new SSH(host, 22, "vagrant", sshKey);
                shellPlain = new Shell.Plain(hostShell);
                if(!shellPlains.containsKey(shellPlain))
                    shellPlains.put(host,shellPlain);
                wait=1;
            } catch (UnknownHostException e) {
                //e.printStackTrace();

            }
            Wait(wait);
        }
        Boolean executed=false;
        log.trace("Waiting for command executon on "+host);
        wait = 2000;
        while(!executed) {
            try {
                String stdout = shellPlain.exec(command);
                executed = true;
                wait = 1;
            } catch (IOException e) {

            }
            Wait(wait);
        }

    }

    public void switchScheduler(String name){
        log.info("Switching scheduler to "+name);
        String newString = "\""+ schedulers.get(name)+"\"";
        for (String schedName: schedulers.keySet())
            if(!schedName.equals(name)){
                String oldString = "\""+ schedulers.get(schedName)+"\"";
                executeCommand(masterHost, "sudo sed -i \"s/"+oldString+"/"+newString+"/g\" /opt/storm/defaults.yaml");
            }
        executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
        killAllProcesses();
        WaitForJSON(baseUrl + "topology/summary", "topologies");

    }

    public String submitTopology(int kbsize, int workers, int emitters, int processors  ){
        String topoName = "patient "+workers+" "+kbsize+" "+emitters+" "+processors;
        log.info("Submitting topology "+topoName);
        executeCommand(masterHost,"/opt/storm/bin/storm jar /opt/storm-examples/seizure-light-2.0.0-SNAPSHOT-jar-with-dependencies.jar seizurelight.SeizurePredictionTopology "+topoName);
        return topoName.replace(" ","_");
    }

    public List<String> GetTopologyIDs(){
        List<String> ret = new ArrayList<String>();
        log.trace("Waiting for topologies list");
        JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");
        for(Object topology : topologies){
            String id = ((JSONObject) topology).get("id").toString();
            ret.add(id);
        }
        return ret;
    }

    public void killTopologies(){
        log.info("Killing existing topologies");
        for(String topoID : GetTopologyIDs()){
            killTopology(topoID.split("-")[0]);
        }
    }
    public void killTopology(String topologyName){
        log.info("Killing topology "+topologyName);
        executeCommand(masterHost,"/opt/storm/bin/storm kill "+topologyName);
    }


    public void killAllProcesses(){
        log.info("Killing unkilled storm processes");
        for (String slaveHost: slaveHosts) {
            executeCommand(slaveHost, "sudo killall java && sudo rm -rf /opt/storm/storm-local");
            //executeCommand(slaveHost,"sudo rm -rf /opt/storm/storm-local");
        }
//        Integer topos = 1;
//        Integer wait = 5000;
//        while(topos>0){
//            try {
//                JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");
//                topos = topologies.length();
//                if(topos==0)
//                    wait=1;
//            } catch (Exception e) {
//
//            }
//            Wait(wait);
//        }
    }

    public JSONArray WaitForJSON(String url, String property){
        //log.trace("Waiting for "+property);
        JSONArray topologies = null;
        while(topologies==null) {
            try{
                JSONObject summary = Utils.getJsonFromUrl(url);
                topologies = (JSONArray) summary.get(property);
            }
            catch (Exception e){
                Wait(5000);
            }
        }
        return topologies;
    }

    public void Wait(int milliseconds){
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e2){

        }
    }
}
