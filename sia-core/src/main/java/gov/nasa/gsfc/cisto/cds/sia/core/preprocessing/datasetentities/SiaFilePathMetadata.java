package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaFilePathCompositeKey;

import javax.persistence.*;
import java.io.Serializable;

/**
 * The type Sia file path metadata.
 *
 * @param <T> the type parameter
 */
@MappedSuperclass
public abstract class SiaFilePathMetadata<T extends SiaMetadata> implements Serializable {

    @EmbeddedId
    private SiaFilePathCompositeKey siaFilePathCompositeKey;
    @ManyToOne
    @JoinColumn(name = "collection_name", referencedColumnName = "collection_name", insertable = false, updatable = false)
    private T siaMetadata;
    @Column(name = "file_path", nullable = false)
    private String filePath;

    /**
     * Instantiates a new Sia file path metadata.
     */
    public SiaFilePathMetadata() {

    }

    /**
     * Instantiates a new Sia file path metadata.
     *
     * @param siaMetadata             the sia metadata
     * @param siaFilePathCompositeKey the sia file path composite key
     * @param filePath                the file path
     */
    public SiaFilePathMetadata(T siaMetadata, SiaFilePathCompositeKey siaFilePathCompositeKey, String filePath) {
        this.siaMetadata = siaMetadata;
        this.siaFilePathCompositeKey = siaFilePathCompositeKey;
        this.filePath = filePath;
    }

    /**
     * Gets sia file path composite key.
     *
     * @return the sia file path composite key
     */
    public SiaFilePathCompositeKey getSiaFilePathCompositeKey() {
        return siaFilePathCompositeKey;
    }

    /**
     * Sets sia file path composite key.
     *
     * @param siaFilePathCompositeKey the sia file path composite key
     */
    public void setSiaFilePathCompositeKey(SiaFilePathCompositeKey siaFilePathCompositeKey) {
        this.siaFilePathCompositeKey = siaFilePathCompositeKey;
    }

    /**
     * Gets sia metadata.
     *
     * @return the sia metadata
     */
    public T getSiaMetadata() {
        return siaMetadata;
    }

    /**
     * Sets sia metadata.
     *
     * @param siaMetadata the sia metadata
     */
    public void setSiaMetadata(T siaMetadata) {
        this.siaMetadata = siaMetadata;
    }

    /**
     * Gets file path.
     *
     * @return the file path
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * Sets file path.
     *
     * @param filePath the file path
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
