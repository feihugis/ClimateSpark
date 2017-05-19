package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


/**
 * The type Sia metadata.
 *
 * @param <S> the type parameter
 * @param <U> the type parameter
 */
@MappedSuperclass
public abstract class SiaMetadata<S extends SiaVariableMetadata, U extends SiaFilePathMetadata> implements Serializable {

    @Id
    @Basic(optional = false)
    @Column(name = "collection_name", nullable = false, updatable = false)
    private String collectionName;
    @Column(name = "temporal_resolution")
    private String temporalResolution;
    @Temporal(TemporalType.DATE)
    @Column(name = "temporal_range_start")
    private Date temporalRangeStart;
    @Temporal(TemporalType.DATE)
    @Column(name = "temporal_range_end")
    private Date temporalRangeEnd;
    @Column(name = "raw_data_format")
    private String rawDataFormat;
    @Column(name = "statistical_interval_type")
    private String statisticalIntervalType;
    @OneToMany(mappedBy = "siaMetadata", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @PrimaryKeyJoinColumn
    private Set<S> siaVariableMetadataSet = new HashSet<S>(0);
    @OneToMany(mappedBy = "siaMetadata", cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @PrimaryKeyJoinColumn
    private Set<U> siaFilePathMetadataSet = new HashSet<U>(0);

    /**
     * Instantiates a new Sia metadata.
     */
    public SiaMetadata() {

    }

    /**
     * Instantiates a new Sia metadata.
     *
     * @param collectionName          the collection name
     * @param temporalResolution      the temporal resolution
     * @param temporalRangeStart      the temporal range start
     * @param temporalRangeEnd        the temporal range end
     * @param rawDataFormat           the raw data format
     * @param statisticalIntervalType the statistical interval type
     * @param siaVariableMetadataSet  the sia variable metadata set
     * @param siaFilePathMetadataSet  the sia file path metadata set
     */
    public SiaMetadata(String collectionName, String temporalResolution, Date temporalRangeStart,
                       Date temporalRangeEnd, String rawDataFormat,
                       String statisticalIntervalType,
                       Set<S> siaVariableMetadataSet,
                       Set<U> siaFilePathMetadataSet) {
        this.collectionName = collectionName;
        this.temporalResolution = temporalResolution;
        this.temporalRangeStart = temporalRangeStart;
        this.temporalRangeEnd = temporalRangeEnd;
        this.rawDataFormat = rawDataFormat;
        this.statisticalIntervalType = statisticalIntervalType;
        this.siaVariableMetadataSet = siaVariableMetadataSet;
        this.siaFilePathMetadataSet = siaFilePathMetadataSet;
    }

    /**
     * Gets collection name.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Sets collection name.
     *
     * @param collectionName the collection name
     */
    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    /**
     * Gets temporal resolution.
     *
     * @return the temporal resolution
     */
    public String getTemporalResolution() { return temporalResolution; }

    /**
     * Sets temporal resolution.
     *
     * @param temporalResolution the temporal resolution
     */
    public void setTemporalResolution(String temporalResolution) { this.temporalResolution = temporalResolution; }

    /**
     * Gets temporal range start.
     *
     * @return the temporal range start
     */
    public Date getTemporalRangeStart() {
        return temporalRangeStart;
    }

    /**
     * Sets temporal range start.
     *
     * @param temporalRangeStart the temporal range start
     */
    public void setTemporalRangeStart(Date temporalRangeStart) {
        this.temporalRangeStart = temporalRangeStart;
    }

    /**
     * Gets temporal range end.
     *
     * @return the temporal range end
     */
    public Date getTemporalRangeEnd() {
        return temporalRangeEnd;
    }

    /**
     * Sets temporal range end.
     *
     * @param temporalRangeEnd the temporal range end
     */
    public void setTemporalRangeEnd(Date temporalRangeEnd) {
        this.temporalRangeEnd = temporalRangeEnd;
    }

    /**
     * Gets raw data format.
     *
     * @return the raw data format
     */
    public String getRawDataFormat() {
        return rawDataFormat;
    }

    /**
     * Sets raw data format.
     *
     * @param rawDataFormat the raw data format
     */
    public void setRawDataFormat(String rawDataFormat) {
        this.rawDataFormat = rawDataFormat;
    }

    /**
     * Gets statistical interval type.
     *
     * @return the statistical interval type
     */
    public String getStatisticalIntervalType() {
        return statisticalIntervalType;
    }

    /**
     * Sets statistical interval type.
     *
     * @param statisticalIntervalType the statistical interval type
     */
    public void setStatisticalIntervalType(String statisticalIntervalType) {
        this.statisticalIntervalType = statisticalIntervalType;
    }

    /**
     * Gets sia variable metadata set.
     *
     * @return the sia variable metadata set
     */
    public Set<S> getSiaVariableMetadataSet() {
        return siaVariableMetadataSet;
    }

    /**
     * Sets sia variable metadata set.
     *
     * @param siaVariableMetadataSet the sia variable metadata set
     */
    public void setSiaVariableMetadataSet(Set<S> siaVariableMetadataSet) {
        this.siaVariableMetadataSet = siaVariableMetadataSet;
    }

    /**
     * Gets sia file path metadata set.
     *
     * @return the sia file path metadata set
     */
    public Set<U> getSiaFilePathMetadataSet() {
        return siaFilePathMetadataSet;
    }

    /**
     * Sets sia file path metadata set.
     *
     * @param siaFilePathMetadataSet the sia file path metadata set
     */
    public void setSiaFilePathMetadataSet(Set<U> siaFilePathMetadataSet) {
        this.siaFilePathMetadataSet = siaFilePathMetadataSet;
    }
}