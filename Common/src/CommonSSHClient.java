package ifmo.escience.dapris.monitoring.common;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

import com.jcabi.ssh.Shell;
import com.jcabi.ssh.SSH;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Pavel Smirnov
 */
public class CommonSSHClient {
    private static Logger log = LogManager.getLogger(CommonSSHClient.class);

    private Shell.Plain shellPlain;
    private String host;
    private String username;
    private String sshKey;


    public CommonSSHClient(String host, String username, String sshKey){
        this.host = host;
        this.username = username;
        this.sshKey = sshKey;
        Open();
    }

    public static String ReadSShKey(String path){
        String ret = "";
        BufferedReader br = null;
        try {
            String sCurrentLine;
            br = new BufferedReader(new FileReader(path));
            while ((sCurrentLine = br.readLine()) != null) {
                if(ret!="")ret+="\n";
                ret+=sCurrentLine;
            }

        } catch (IOException e){
            log.error(Utils.GetTracedError(e));
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException e)
            {
                log.error(Utils.GetTracedError(e));
            }
        }
        return ret;
    }

    private void Open(){

        log.trace("Waiting for ssh conection to "+host);
        Integer wait = 2000;
        while (shellPlain==null){
            try {
                SSH hostShell = new SSH(host, 22, username, sshKey);
                shellPlain = new Shell.Plain(hostShell);
                wait=1;
            } catch (UnknownHostException e) {
                //e.printStackTrace();
                log.error(Utils.GetTracedError(e));
            }
            Utils.Wait(wait);
        }
    }

    public String executeCommand(String command){
        String ret = "";

        if(shellPlain==null)
            Open();

        log.trace(host+" -> "+command);
        Boolean executed=false;
        log.trace("Waiting for command executon on "+host);
        Integer wait = 2000;
        while(!executed) {
            try {
                ret = shellPlain.exec(command);
                if(ret.contains("Exception"))
                    log.error(ret);
                else{
                    executed = true;
                    wait = 1;
                }
            } catch (IOException e) {

            }
            Utils.Wait(wait);
        }
        return ret;
    }

}
