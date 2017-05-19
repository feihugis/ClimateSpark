package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

/**
 * The type Merra variable metadata.
 */
@Entity
@Table(name = "merra_variable_metadata")
@AssociationOverrides({
        @AssociationOverride(name = "siaMetadata", joinColumns = @JoinColumn(name = "collection_name", insertable = false, updatable = false))
})
public class MerraVariableMetadata extends SiaVariableMetadata<MerraMetadata> {

    private MerraMetadata siaMetadata;

    /**
     * Instantiates a new Merra variable metadata.
     */
    public MerraVariableMetadata() {

    }

}
