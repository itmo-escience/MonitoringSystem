package baseEntities;

import entities.AlgorithmParameter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author Alexander Visheratin
 */
public abstract class BaseAlgorithm implements Serializable {

    public BaseEnvironment environment;
    public BaseSchedule currentSchedule;
    public List<AlgorithmParameter> parameters;
    
    public void BaseAlgorithm(){
        
    }
    
    public void BaseAlgorithm(BaseEnvironment env, BaseSchedule schedule, 
            List<AlgorithmParameter> params){
        environment = env;
        currentSchedule = schedule;
        parameters = params;
    }
    
    public List<AlgorithmParameter> getParameters(){
        List<AlgorithmParameter> result = new ArrayList<AlgorithmParameter>();
        for(AlgorithmParameter param : parameters)
            result.add(param.clone());
        return result;
    }
    
    public void setParameters(List<AlgorithmParameter> newParameters){
        parameters = newParameters;
    }
    
    public abstract void run();
}
