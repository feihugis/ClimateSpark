package gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetentities;

import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaVariableCompositeKey;

import javax.persistence.*;
import java.io.Serializable;
import java.util.List;


/**
 * The type Sia variable metadata.
 *
 * @param <T> the type parameter
 */
@MappedSuperclass
public abstract class SiaVariableMetadata<T extends SiaMetadata> implements Serializable {

  @EmbeddedId
  private SiaVariableCompositeKey siaVariableCompositeKey;
  @ManyToOne
  @JoinColumn(name = "collection_name", referencedColumnName = "collection_name", insertable = false, updatable = false)
  private T siaMetadata;
  @Column(name = "spatial_resolution", nullable = false)
  @ElementCollection(targetClass = Float.class)
  private List<Float> spatialResolution;
  @Column(name = "temporal_resolution", nullable = false)
  @ElementCollection(targetClass = Integer.class)
  private List<Integer> temporalResolution;
  @Column(name = "dimension_order", nullable = false)
  @ElementCollection(targetClass = String.class)
  private List<String> dimensionOrder;
  @Column(name = "chunk_sizes")
  @ElementCollection(targetClass = Integer.class)
  private List<Integer> chunkSizes;
  @Column(name = "valid_min")
  private Float validMin;
  @Column(name = "valid_max")
  private Float validMax;
  @Column(name = "missing_value")
  private Float missingValue;
  @Column(name = "fill_value")
  private Float fillValue;
  @Column(name = "add_offset")
  private Float addOffset;
  @Column(name = "scale_factor")
  private Float scaleFactor;
  @Column(name = "units")
  private String units;
  @Column(name = "dataType")
  private String dataType;

  /**
   * Instantiates a new Sia variable metadata.
   */
  public SiaVariableMetadata() {

  }

  /**
   * Instantiates a new Sia variable metadata.
   *
   * @param siaMetadata             the sia metadata
   * @param siaVariableCompositeKey the sia variable composite key
   * @param spatialResolution       the spatial resolution
   * @param temporalResolution      the temporal resolution
   * @param dimensionOrder          the dimension order
   * @param validMin                the valid min
   * @param validMax                the valid max
   * @param missingValue            the missing value
   * @param fillValue               the fill value
   * @param addOffset               the add offset
   * @param scaleFactor             the scale factor
   * @param units                   the units
   * @param chunkSizes              the chunk sizes
   */
  public SiaVariableMetadata(T siaMetadata, SiaVariableCompositeKey siaVariableCompositeKey, List<Float> spatialResolution, List<Integer> temporalResolution, List<String> dimensionOrder, Float validMin, Float validMax, Float missingValue, Float fillValue, Float addOffset, Float scaleFactor, String units, List<Integer> chunkSizes) {
    this.siaMetadata = siaMetadata;
    this.siaVariableCompositeKey = siaVariableCompositeKey;
    this.spatialResolution = spatialResolution;
    this.temporalResolution = temporalResolution;
    this.dimensionOrder = dimensionOrder;
    this.validMin = validMin;
    this.validMax = validMax;
    this.missingValue = missingValue;
    this.fillValue = fillValue;
    this.addOffset = addOffset;
    this.scaleFactor = scaleFactor;
    this.units = units;
    this.chunkSizes = chunkSizes;
  }

  /**
   * Gets sia variable composite key.
   *
   * @return the sia variable composite key
   */
  public SiaVariableCompositeKey getSiaVariableCompositeKey() { return siaVariableCompositeKey; }

  /**
   * Sets sia variable composite key.
   *
   * @param siaVariableCompositeKey the sia variable composite key
   */
  public void setSiaVariableCompositeKey(SiaVariableCompositeKey siaVariableCompositeKey) { this.siaVariableCompositeKey = siaVariableCompositeKey; }


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
   * Gets spatial resolution.
   *
   * @return the spatial resolution
   */
  public List<Float> getSpatialResolution() {
    return spatialResolution;
  }

  /**
   * Sets spatial resolution.
   *
   * @param spatialResolution the spatial resolution
   */
  public void setSpatialResolution(List<Float> spatialResolution) {
    this.spatialResolution = spatialResolution;
  }

  /**
   * Gets temporal resolution.
   *
   * @return the temporal resolution
   */
  public List<Integer> getTemporalResolution() {
    return temporalResolution;
  }

  /**
   * Sets temporal resolution.
   *
   * @param temporalResolution the temporal resolution
   */
  public void setTemporalResolution(List<Integer> temporalResolution) {
    this.temporalResolution = temporalResolution;
  }

  /**
   * Gets dimension order.
   *
   * @return the dimension order
   */
  public List<String> getDimensionOrder() {
    return dimensionOrder;
  }

  /**
   * Sets dimension order.
   *
   * @param dimensionOrder the dimension order
   */
  public void setDimensionOrder(List<String> dimensionOrder) {
    this.dimensionOrder = dimensionOrder;
  }

  /**
   * Gets chunk sizes.
   *
   * @return the chunk sizes
   */
  public List<Integer> getChunkSizes() {
    return chunkSizes;
  }

  /**
   * Sets chunk sizes.
   *
   * @param chunkSizes the chunk sizes
   */
  public void setChunkSizes(List<Integer> chunkSizes) {
    this.chunkSizes = chunkSizes;
  }

  /**
   * Gets valid min.
   *
   * @return the valid min
   */
  public Float getValidMin() {
    return validMin;
  }

  /**
   * Sets valid min.
   *
   * @param validMin the valid min
   */
  public void setValidMin(Float validMin) {
    this.validMin = validMin;
  }

  /**
   * Gets valid max.
   *
   * @return the valid max
   */
  public Float getValidMax() {
    return validMax;
  }

  /**
   * Sets valid max.
   *
   * @param validMax the valid max
   */
  public void setValidMax(Float validMax) {
    this.validMax = validMax;
  }

  /**
   * Gets missing value.
   *
   * @return the missing value
   */
  public Float getMissingValue() { return missingValue; }

  /**
   * Sets missing value.
   *
   * @param missingValue the missing value
   */
  public void setMissingValue(Float missingValue) { this.missingValue = missingValue; }

  /**
   * Gets fill value.
   *
   * @return the fill value
   */
  public Float getFillValue() {
    return fillValue;
  }

  /**
   * Sets fill value.
   *
   * @param fillValue the fill value
   */
  public void setFillValue(Float fillValue) {
    this.fillValue = fillValue;
  }

  /**
   * Gets add offset.
   *
   * @return the add offset
   */
  public Float getAddOffset() {
    return addOffset;
  }

  /**
   * Sets add offset.
   *
   * @param addOffset the add offset
   */
  public void setAddOffset(Float addOffset) {
    this.addOffset = addOffset;
  }

  /**
   * Gets scale factor.
   *
   * @return the scale factor
   */
  public Float getScaleFactor() {
    return scaleFactor;
  }

  /**
   * Sets scale factor.
   *
   * @param scaleFactor the scale factor
   */
  public void setScaleFactor(Float scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  /**
   * Gets units.
   *
   * @return the units
   */
  public String getUnits() {
    return units;
  }

  /**
   * Sets units.
   *
   * @param units the units
   */
  public void setUnits(String units) {
    this.units = units;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }
}