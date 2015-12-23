package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class CustomTreeMap <K,V> extends TreeMap<K,V> {
    @Id
    private String id;
    public CustomTreeMap(String id){
        this.id=id;

    }
    public String getId(){ return id; }
    public void setId(String id){ this.id=id; }
}
