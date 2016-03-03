package ifmo.escience.dapris.monitoring.computationsMonitor.StateStructures;

import org.javers.core.metamodel.annotation.Id;

/**
 * Created by Pavel Smirnov
 */
public class IdentifiedObject {

        @Id
        private String id;
        private Double object;
        public IdentifiedObject(String id,Double object){
            this.id=id;
            this.object=object;
        }
        public String getId(){ return id; }
        public void setId(String id){ this.id=id; }

}
