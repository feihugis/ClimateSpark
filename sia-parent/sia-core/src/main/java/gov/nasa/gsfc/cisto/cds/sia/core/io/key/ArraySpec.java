package gov.nasa.gsfc.cisto.cds.sia.core.io.key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * The type Array spec.
 */
@SuppressWarnings("rawtypes")
public class ArraySpec implements WritableComparable {

	private String _fileName = "";
	private String _varName = "";
	private int[] _shape = new int[3];
	private int[] _corner = new int[3];
	private int[] _varShape = new int[3];
	private int[] _logicalStartOffset = new int[3];
	private Float _validMax = 1.0E30F;
	private Float _validMin = -1.0E30F;
	private Float _fillValue = 9.9999999E14F;
	private String _date = "";
	private int _boundaryBoxType = 0;
	private long offset = 0L;
	private long length = 0L;

    /**
     * Instantiates a new Array spec.
     */
    public ArraySpec() {
	}

    /**
     * Instantiates a new Array spec.
     *
     * @param corner        the corner
     * @param shape         the shape
     * @param varName       the var name
     * @param fileName      the file name
     * @param variableShape the variable shape
     * @throws Exception the exception
     */
    public ArraySpec(int[] corner, int[] shape, String varName, String fileName, int[] variableShape) throws Exception {
		if (shape.length != corner.length) {
			throw new Exception("shape and length need to be of the same length");
		}

		this._shape = new int[shape.length];
		System.arraycopy(shape, 0, this._shape, 0, shape.length);

		this._corner = new int[corner.length];
		System.arraycopy(corner, 0, this._corner, 0, corner.length);

		this._varShape = new int[variableShape.length];
		System.arraycopy(variableShape, 0, this._varShape, 0, variableShape.length);

		this._varName = varName;
		this._fileName = fileName;
	}

    /**
     * Instantiates a new Array spec.
     *
     * @param corner   the corner
     * @param shape    the shape
     * @param varName  the var name
     * @param fileName the file name
     * @throws Exception the exception
     */
    public ArraySpec(int[] corner, int[] shape, String varName, String fileName) throws Exception {
		this(corner, shape, varName, fileName, new int[0]);
	}

    /**
     * Instantiates a new Array spec.
     *
     * @param corner  the corner
     * @param shape   the shape
     * @param varName the var name
     * @throws Exception the exception
     */
    public ArraySpec(int[] corner, int[] shape, String varName) throws Exception {
		if (shape.length != corner.length) {
			throw new Exception("shape and length need to be of the same length");
		}

		this._shape = new int[shape.length];
		System.arraycopy(shape, 0, this._shape, 0, shape.length);

		this._corner = new int[corner.length];
		System.arraycopy(corner, 0, this._corner, 0, corner.length);

		this._varName = varName;
	}

    /**
     * Instantiates a new Array spec.
     *
     * @param corner    the corner
     * @param shape     the shape
     * @param validMax  the valid max
     * @param validMin  the valid min
     * @param fillValue the fill value
     */
    public ArraySpec(int[] corner, int[] shape, float validMax, float validMin, float fillValue) {
		this._shape = new int[shape.length];
		System.arraycopy(shape, 0, this._shape, 0, shape.length);

		this._corner = new int[corner.length];
		System.arraycopy(corner, 0, this._corner, 0, corner.length);

		this._validMax = validMax;
		this._validMin = validMin;
		this._fillValue = fillValue;
	}

    /**
     * Gets rank.
     *
     * @return the rank
     */
    public int getRank() {
		return this._shape.length;
	}

    /**
     * Get corner int [ ].
     *
     * @return the int [ ]
     */
    public int[] getCorner() {
		return this._corner;
	}

    /**
     * Get shape int [ ].
     *
     * @return the int [ ]
     */
    public int[] getShape() {
		return this._shape;
	}

    /**
     * Sets shape.
     *
     * @param newShape the new shape
     */
    public void setShape(int[] newShape) {
		this._shape = newShape;
	}

    /**
     * Get variable shape int [ ].
     *
     * @return the int [ ]
     */
    public int[] getVariableShape() {
		return this._varShape;
	}

    /**
     * Sets variable shape.
     *
     * @param newVarShape the new var shape
     */
    public void setVariableShape(int[] newVarShape) {
		this._varShape = newVarShape;
	}

    /**
     * Get logical start offset int [ ].
     *
     * @return the int [ ]
     */
    public int[] getLogicalStartOffset() {
		return this._logicalStartOffset;
	}

    /**
     * Sets logical start offset.
     *
     * @param newLogicalStartOffset the new logical start offset
     */
    public void setLogicalStartOffset(int[] newLogicalStartOffset) {
		this._logicalStartOffset = newLogicalStartOffset;
	}

    /**
     * Gets var name.
     *
     * @return the var name
     */
    public String getVarName() {
		return this._varName;
	}

    /**
     * Sets var name.
     *
     * @param varName the var name
     */
    public void setVarName(String varName) {
		this._varName = varName;
	}

    /**
     * Gets file name.
     *
     * @return the file name
     */
    public String getFileName() {
		return this._fileName;
	}

    /**
     * Gets size.
     *
     * @return the size
     */
    public long getSize() {
		long size = 1L;
		for (int a_shape : this._shape) {
			size *= a_shape;
		}

		return size;
	}

    /**
     * Gets offset.
     *
     * @return the offset
     */
    public long getOffset() {
		return this.offset;
	}

    /**
     * Sets offset.
     *
     * @param offset the offset
     */
    public void setOffset(long offset) {
		this.offset = offset;
	}

    /**
     * Gets length.
     *
     * @return the length
     */
    public long getLength() {
		return this.length;
	}

    /**
     * Sets length.
     *
     * @param length the length
     */
    public void setLength(long length) {
		this.length = length;
	}

	public String toString() {
		return

		this._fileName + ": var: " + this._varName + ": corner = " + Arrays.toString(this._corner) + ", shape = "
				+ Arrays.toString(this._shape);
	}

	public int compareTo(Object o) {
		int retVal = 0;
		ArraySpec other = (ArraySpec) o;

		if (this._fileName.compareTo(other.getFileName()) != 0) {
			return this._fileName.compareTo(other.getFileName());
		}

		if (this._varName.compareTo(other.getVarName()) != 0) {
			return this._varName.compareTo(other.getVarName());
		}

		for (int i = 0; i < this._corner.length; i++) {
			retVal = this._corner[i] - other.getCorner()[i];

			if (retVal != 0) {
				return retVal;
			}
		}

		return retVal;
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, this._fileName);
		Text.writeString(out, this._varName);
		Text.writeString(out, this._date);

		out.writeInt(this._shape.length);
		for (int a_shape : this._shape) {
			out.writeInt(a_shape);
		}
		out.writeInt(this._corner.length);
		for (int a_corner : this._corner) {
			out.writeInt(a_corner);
		}
		out.writeInt(this._varShape.length);
		for (int a_varShape : this._varShape) {
			out.writeInt(a_varShape);
		}
		if (this._logicalStartOffset == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this._logicalStartOffset.length);
			for (int a_logicalStartOffset : this._logicalStartOffset) {
				out.writeInt(a_logicalStartOffset);
			}
		}

		out.writeFloat(this._validMax.floatValue());
		out.writeFloat(this._validMin.floatValue());
		out.writeFloat(this._fillValue.floatValue());
		out.writeInt(this._boundaryBoxType);
		out.writeLong(this.offset);
		out.writeLong(this.length);
	}

	public void readFields(DataInput in) throws IOException {
		this._fileName = Text.readString(in);
		this._varName = Text.readString(in);
		this._date = Text.readString(in);

		int len = in.readInt();
		this._shape = new int[len];
		for (int i = 0; i < this._shape.length; i++) {
			this._shape[i] = in.readInt();
		}
		len = in.readInt();
		this._corner = new int[len];
		for (int i = 0; i < this._corner.length; i++) {
			this._corner[i] = in.readInt();
		}
		len = in.readInt();
		this._varShape = new int[len];
		for (int i = 0; i < this._varShape.length; i++) {
			this._varShape[i] = in.readInt();
		}
		len = in.readInt();
		if (len == 0) {
			this._logicalStartOffset = null;
		} else {
			this._logicalStartOffset = new int[len];
			for (int i = 0; i < this._logicalStartOffset.length; i++) {
				this._logicalStartOffset[i] = in.readInt();
			}
		}
		this._validMax = in.readFloat();
		this._validMin = in.readFloat();
		this._fillValue = in.readFloat();
		this._boundaryBoxType = in.readInt();
		this.offset = in.readLong();
		this.length = in.readLong();
	}

    /**
     * Gets valid max.
     *
     * @return the valid max
     */
    public Float getValidMax() {
		return this._validMax;
	}

    /**
     * Sets valid max.
     *
     * @param validMax the valid max
     */
    public void setValidMax(Float validMax) {
		this._validMax = validMax;
	}

    /**
     * Gets valid min.
     *
     * @return the valid min
     */
    public Float getValidMin() {
		return this._validMin;
	}

    /**
     * Sets valid min.
     *
     * @param validMin the valid min
     */
    public void setValidMin(Float validMin) {
		this._validMin = validMin;
	}

    /**
     * Gets fill value.
     *
     * @return the fill value
     */
    public Float getFillValue() {
		return this._fillValue;
	}

    /**
     * Sets fill value.
     *
     * @param fillValue the fill value
     */
    public void setFillValue(Float fillValue) {
		this._fillValue = fillValue;
	}

    /**
     * Gets boundary box type.
     *
     * @return the boundary box type
     */
    public int getBoundaryBoxType() {
		return this._boundaryBoxType;
	}

    /**
     * Sets boundary box type.
     *
     * @param type the type
     */
    public void setBoundaryBoxType(int type) {
		this._boundaryBoxType = type;
	}

    /**
     * Gets date.
     *
     * @return the date
     */
    public String getDate() {
		return _date;
	}

    /**
     * Sets date.
     *
     * @param _date the date
     */
    public void setDate(String _date) {
		this._date = _date;
	}

}

/*
 * Location:
 * /Users/mkbowen/Desktop/MerraTest.jar!/edu/gmu/stc/merra/hadoop/io/ArraySpec.
 * class Java compiler version: 6 (50.0) JD-Core Version: 0.7.1
 */