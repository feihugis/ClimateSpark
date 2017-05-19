package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

/**
 * The type Merra 2 variable metadata.
 */
@Entity
@Table(name = "merra2_variable_metadata")
@AssociationOverrides({
        @AssociationOverride(name = "siaMetadata", joinColumns = @JoinColumn(name = "collection_name", insertable = false, updatable = false))
})
public class Merra2VariableMetadata extends SiaVariableMetadata<Merra2Metadata> {

    private Merra2Metadata siaMetadata;

    /**
     * Instantiates a new Merra 2 variable metadata.
     */
    public Merra2VariableMetadata() {

    }

}
