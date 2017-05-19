package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;
import java.util.Set;

/**
 * The type Merra 2 metadata.
 */
@Entity
@Table(name = "merra2_metadata")
@AssociationOverrides({
        @AssociationOverride(name = "siaVariableMetadataSet", joinColumns = @JoinColumn(name = "collection_name")),
        @AssociationOverride(name = "siaFilePathMetadataSet", joinColumns = @JoinColumn(name = "collection_name"))
})
public class Merra2Metadata extends SiaMetadata<Merra2VariableMetadata, Merra2FilePathMetadata> {

    private Set<Merra2VariableMetadata> siaVariableMetadataSet;
    private Set<Merra2FilePathMetadata> siaFilePathMetadataSet;

    /**
     * Instantiates a new Merra 2 metadata.
     */
    public Merra2Metadata() {

    }
}