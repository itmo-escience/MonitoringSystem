package baseEntities;

import entities.NetworkType;
import entities.Node;
import entities.Task;
import entities.TaskType;
import java.io.Serializable;
import java.util.Set;

/**
 *
 * @author Alexander Visheratin
 */
public abstract class BaseEnvironment implements Serializable {
    private Set<Node> nodes;
    private Set<NetworkType> networks;
    private Set<BaseDataLayer> dataLayers;
    private Set<Task> tasks;
    private Set<TaskType> taskTypes;
    
}