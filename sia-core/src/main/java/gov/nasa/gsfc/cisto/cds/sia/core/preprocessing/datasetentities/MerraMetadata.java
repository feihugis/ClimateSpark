package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;
import java.util.Set;

/**
 * The type Merra metadata.
 */
@Entity
@Table(name = "merra_metadata")
@AssociationOverrides({
        @AssociationOverride(name = "siaVariableMetadataSet", joinColumns = @JoinColumn(name = "collection_name")),
        @AssociationOverride(name = "siaFilePathMetadataSet", joinColumns = @JoinColumn(name = "collection_name"))
})
public class MerraMetadata extends SiaMetadata<MerraVariableMetadata,MerraFilePathMetadata> {

    private Set<MerraVariableMetadata> siaVariableMetadataSet;
    private Set<MerraFilePathMetadata> siaFilePathMetadataSet;

    /**
     * Instantiates a new Merra metadata.
     */
    public MerraMetadata() {

    }
}