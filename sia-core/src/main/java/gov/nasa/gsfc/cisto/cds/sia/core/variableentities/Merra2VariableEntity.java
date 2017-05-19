package gov.nasa.gsfc.cisto.cds.sia.core.variableentities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.Merra2VariableAttribute;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;
import org.apache.hadoop.io.Text;

import javax.persistence.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Fei Hu on 8/19/16.
 */
@Entity
public class Merra2VariableEntity implements SiaVariableEntity {
  @Id @GeneratedValue(strategy = GenerationType.AUTO)
  private int id;
  @Column(name = "temporal_component")
  private int temporalComponent;
  @Column(name = "corner")
  private String corner;
  @Column(name = "byte_offset")
  private long byteOffset;
  @Column(name = "byte_length")
  private long byteLength;
  @Column(name = "block_hosts")
  private String blockHosts;
  @Column(name = "geometry_id")
  private int geometryID;
  @Column(name="compression_code")
  private int compressionCode;

  private String varName;

  public void initializeEntity(SiaVariableAttribute variableMetadata) {
    Merra2VariableAttribute merra2VariableMetadata = (Merra2VariableAttribute) variableMetadata;
    this.temporalComponent = merra2VariableMetadata.getTemporalComponent();
    this.varName = merra2VariableMetadata.getVarShortName();
    this.corner = "";
    for(int one: merra2VariableMetadata.getCorner()) {
      this.corner = this.corner + one + ",";
    }
    this.byteOffset = merra2VariableMetadata.getFilePos();
    this.byteLength = merra2VariableMetadata.getByteSize();
    this.blockHosts = merra2VariableMetadata.getBlockHosts();
    this.geometryID = merra2VariableMetadata.getGeometryID();
    System.out.println("------------------- " + merra2VariableMetadata.getGeometryID());
    this.compressionCode = merra2VariableMetadata.getCompressionCode();
  }

  public void setVarName(String varName) {
    this.varName = varName;
  }

  public String getVarName() {
    return this.varName;
  }

  public int getTemporalComponent() {
    return this.temporalComponent;
  }

  public void setTemporalComponent(int temporalComponent) {
    this.temporalComponent = temporalComponent;
  }

  public long getByteOffset() {
    return this.byteOffset;
  }

  public void setByteOffset(long byteOffset) {
    this.byteOffset = byteOffset;
  }

  public long getByteLength() {
    return this.byteLength;
  }

  public void setByteLength(long byteLength) {
    this.byteLength = byteLength;
  }

  public String getBlockHosts() {
    return this.blockHosts;
  }

  public void setBlockHosts(String blockHosts) {
    this.blockHosts = blockHosts;
  }

  public int getCompressionCode() {
    return this.compressionCode;
  }

  public void setCompressionCode(int compressionCode) {
    this.compressionCode = compressionCode;
  }

    /**
     * Gets id.
     *
     * @return the id
     */
    public int getId() {
    return id;
  }

    /**
     * Sets id.
     *
     * @param id the id
     */
    public void setId(int id) {
    this.id = id;
  }

    /**
     * Gets geometry id.
     *
     * @return the geometry id
     */
    public int getGeometryID() {
    return geometryID;
  }

    /**
     * Sets geometry id.
     *
     * @param geometryID the geometry id
     */
    public void setGeometryID(int geometryID) {
    this.geometryID = geometryID;
  }

  public String getCorner() {
    return corner;
  }

    /**
     * Sets corner.
     *
     * @param corner the corner
     */
    public void setCorner(String corner) {
    this.corner = corner;
  }

  public void write(Kryo kryo, Output output) {
    output.writeInt(this.id);
    output.writeInt(this.temporalComponent);
    output.writeString(this.corner);
    output.writeLong(this.byteOffset);
    output.writeLong(this.byteLength);
    output.writeString(this.blockHosts);
    output.writeInt(this.geometryID);
    output.writeInt(this.compressionCode);
  }

  public void read(Kryo kryo, Input input) {
    this.id = input.readInt();
    this.temporalComponent = input.readInt();
    this.corner = input.readString();
    this.byteOffset = input.readLong();
    this.byteLength = input.readLong();
    this.blockHosts = input.readString();
    this.geometryID = input.readInt();
    this.compressionCode = input.readInt();
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(this.id);
    dataOutput.writeInt(this.temporalComponent);
    Text.writeString(dataOutput, this.corner);
    dataOutput.writeLong(this.byteOffset);
    dataOutput.writeLong(this.byteLength);
    Text.writeString(dataOutput, this.blockHosts);
    dataOutput.writeInt(this.geometryID);
    dataOutput.writeInt(this.compressionCode);
  }

  public void readFields(DataInput dataInput) throws IOException {
    this.id = dataInput.readInt();
    this.temporalComponent = dataInput.readInt();
    this.corner = Text.readString(dataInput);
    this.byteOffset = dataInput.readLong();
    this.byteLength = dataInput.readLong();
    this.blockHosts = Text.readString(dataInput);
    this.geometryID = dataInput.readInt();
    this.compressionCode = dataInput.readInt();
  }
}
