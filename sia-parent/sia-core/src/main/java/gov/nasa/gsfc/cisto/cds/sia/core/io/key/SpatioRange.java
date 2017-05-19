package gov.nasa.gsfc.cisto.cds.sia.core.io.key;

/**
 * The type Spatio range.
 */
public class SpatioRange {

    /**
     * The Corner.
     */
    public int[] corner;
    /**
     * The Shape.
     */
    public int[] shape;

    /**
     * Instantiates a new Spatio range.
     *
     * @param corner the corner
     * @param shape  the shape
     */
    public SpatioRange(int[] corner, int[] shape) {
    this.corner = corner;
    this.shape = shape;
  }
}