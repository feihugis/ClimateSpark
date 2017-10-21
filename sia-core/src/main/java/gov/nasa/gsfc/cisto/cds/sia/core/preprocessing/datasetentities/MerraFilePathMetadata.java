package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;

/**
 * The type Merra file path metadata.
 */
@Entity
@Table(name = "merra_file_path_metadata")
@AssociationOverrides({
        @AssociationOverride(name = "siaMetadata", joinColumns = @JoinColumn(name = "collection_name", insertable = false, updatable = false))
})
public class MerraFilePathMetadata extends SiaFilePathMetadata<MerraMetadata> {

    private MerraMetadata siaMetadata;

    /**
     * Instantiates a new Merra file path metadata.
     */
    public MerraFilePathMetadata() {

    }
}
