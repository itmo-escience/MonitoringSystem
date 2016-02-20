import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import com.jcabi.ssh.Shell;
import com.jcabi.ssh.SSH;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
    String experimentsCollection = "storm.experiments";
    String runsCollection = "storm.runs";
    String masterHost = "192.168.92.11";
    String baseUrl = "http://192.168.92.11:8080/api/v1/";
    String[] slaveHosts = new String[]{"192.168.92.13", "192.168.92.14", "192.168.92.15", "192.168.92.16"};
    private HashMap<String, Shell.Plain> shellPlains = new HashMap<String, Shell.Plain>();
    private HashMap<String, String> schedulers = new HashMap<String, String>();

    public static void main(String[] args){
        StormSchedulerExperiments stormMetricsMonitor = new StormSchedulerExperiments("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.killAllProcesses();
        stormMetricsMonitor.startExperiments(2);
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

        log.info("Starting experiments");
        int i=0;
        Document experiment;
        Document expFind;

        for(String scheduler : new String[]{ /*"default", "resource", */ "annealing"}){
            switchScheduler(scheduler);
            for(int megabytes: new int[]{ 3, 5, 10 })
                for(int workers: new int[]{ 5 , 10, 15  })
                    for(int emitters: new int[]{ 1 })
                        for(int processors: new int[]{ 3, 5, 8 }){
                            Integer kbsize = 1024*megabytes;
                            String topoName = "patient";
                            String paramString = kbsize+" "+workers+" "+emitters+" "+processors;
                            String expId = "patient_"+paramString.replace(" ","_");
                            expFind = new Document(){{ put("_id", expId); }};
                            experiment = mongoClient.getDocumentFromDB(experimentsCollection, expFind);

                            if(experiment==null){
                                experiment = new Document();
                                experiment.put("_id",expId);
                                experiment.put("kbSize", kbsize);
                                experiment.put("workers", workers);
                                experiment.put("emitters", emitters);
                                experiment.put("processors", processors);
                                experiment.put("runs", new ArrayList<Document>());
                                mongoClient.insertDocumentToDB(experimentsCollection, experiment);
                            }

                            Document run = new Document();

                            Date runStarted = new Date();
                            run.put("started", runStarted);
                            run.put("expID",expId);
                            run.put("scheduler", scheduler);

                            submitTopology(topoName, paramString);
                            String topoId = getTopoId(expId);

                            run.put("_id",topoId);
                            mongoClient.insertDocumentToDB(runsCollection, run);
                            Document runFind = new Document(){{ put("_id", topoId); }};

                            //((ArrayList<String>)experiment.get("runs")).add(topoId);
                            ((ArrayList<Document>)experiment.get("runs")).add(run);
                            mongoClient.updateDocumentInDB(experimentsCollection, expFind , experiment);

                            mongoClient.updateDocumentInDB(runsCollection, runFind , run);

                            log.info("Doing run with "+scheduler+" scheduler ("+String.valueOf(seconds)+") seconds");
                            Wait(seconds * 1000);

                            run.put("finished", new Date());
                            mongoClient.updateDocumentInDB(runsCollection, runFind, run);
                            killTopology(topoName);
                            killAllProcesses();
                            capturePicture(topoId);
                            i++;
                        }


        }
        killTopologies();

    }


    public String getTopoId(String topoName){
        log.info("Waiting for submitted topology ID");
        String topoId=null;
        Integer wait = 5000;
        while(topoId==null){
            try{

                JSONArray topologies = getTopologies();
                for (Object topology : topologies){
                    String name = ((JSONObject)topology).get("name").toString();
                    if(name.equals(topoName)){
                        topoId =  ((JSONObject)topology).get("id").toString();
                        wait=1;
                        break;
                    }
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

    public String executeCommand(String host, String command){
        String ret = "";
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
                ret = shellPlain.exec(command);
                executed = true;
                wait = 1;
            } catch (IOException e) {

            }
            Wait(wait);
        }
        return ret;
    }

    public void switchScheduler(String name){
        log.info("Switching scheduler to "+name);

        String oldConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
        for (String schedName: schedulers.keySet())
            if(!schedName.equals(name)){
                executeCommand(masterHost, "sudo sed -i \"s/"+schedulers.get(schedName)+"/"+schedulers.get(name)+"/g\" /opt/storm/defaults.yaml");
            }
        //Wait(1000);
        String newConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
        if(!newConfig.equals(oldConfig))
            executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
        //executeCommand(masterHost,"sudo service supervisor restart`");
        getSupervisorIDs();

    }

    public void submitTopology(String topoName, String paramString){
        log.info("Submitting topology "+topoName);
        String command = "/opt/storm/bin/storm jar /opt/storm-examples/seizure-light-2.0.0-SNAPSHOT-jar-with-dependencies.jar seizurelight.SeizurePredictionTopology "+ topoName+" "+paramString;
        executeCommand(masterHost, command);

    }

    public JSONArray getTopologies(){
        log.trace("Waiting for topologies list");
        JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");

        return topologies;
    }

    public List<String> getSupervisorIDs(){
        List<String> ret = new ArrayList<String>();
        log.trace("Waiting for supervisors list");
        JSONArray topologies = WaitForJSON(baseUrl + "supervisor/summary", "supervisors");
        for(Object topology : topologies){
            String id = ((JSONObject) topology).get("id").toString();
            ret.add(id);
        }
        return ret;
    }

    public void capturePicture(String topoId){
        String path = "d:/Projects/MonitoringSystem/StormMetricsMonitor/target/img/load";
        try {
            URL website = new URL("http://192.168.92.11/ganglia/stacked.php?m=load_one&c=ds1&r=hour&st=1455921590");
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(path+"/"+topoId+".png");
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void killTopologies(){
        log.info("Killing existing topologies");
        for(Object topology : getTopologies()){
            if(!((JSONObject)topology).get("status").toString().equals("KILLED"))
                killTopology(((JSONObject)topology).get("name").toString());
        }
        killAllProcesses();
    }
    public void killTopology(String topologyName){
        log.info("Killing topology "+topologyName);
        executeCommand(masterHost,"/opt/storm/bin/storm kill "+topologyName);
    }

    public void killAllProcesses(){
        log.info("Killing worker processes");
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
        int i=0;
        int wait=1000;
        JSONArray topologies = null;
        while(topologies==null) {
            try{
                JSONObject json = Utils.getJsonFromUrl(url);
                if(json!=null){
                    topologies = (JSONArray) json.get(property);
                    wait = 1;
                }
            }
            catch (Exception e){

            }
            if(i>0 && i%2==i/2)
                executeCommand(masterHost,"cd - && sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
            i++;
            Wait(wait);
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
