package gov.nasa.gsfc.cisto.cds.sia.core.variableentities;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.MerraVariableAttribute;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;

import javax.persistence.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import com.sun.org.apache.xpath.internal.operations.String;

/**
 * The type Merra variable entity.
 */
@Entity
public class MerraVariableEntity implements SiaVariableEntity {
    @Id @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;
    @Column(name="temporal_component")
    private int temporalComponent;
    @Column(name="block_hosts")
    private String blockHosts;
    @Column(name="byte_offset")
    private long byteOffset;
    @Column(name="byte_length")
    private long byteLength;
    @Column(name="compression_code")
    private int compressionCode;
    @Column(name = "corner")
    private String corner;

    private String varName;

  public void initializeEntity(SiaVariableAttribute siaVariableAttribute) {
      MerraVariableAttribute merraVariableAttribute = (MerraVariableAttribute) siaVariableAttribute;
      this.temporalComponent = merraVariableAttribute.getTemporalComponent();
      this.byteOffset = merraVariableAttribute.getByteOffset();
      this.byteLength = merraVariableAttribute.getByteLength();
      this.blockHosts = merraVariableAttribute.getBlockHosts();
      this.compressionCode = merraVariableAttribute.getCompressionCode();
      this.corner = "";
      for(int one: merraVariableAttribute.getCorner()) {
        this.corner = this.corner + one + ",";
      }
  }

  public void setVarName(String varName) {
      this.varName = varName;
  }

  public String getVarName() {
    return this.varName;
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

    public int getTemporalComponent() {
        return temporalComponent;
    }

    public void setTemporalComponent(int temporalComponent) {
        this.temporalComponent = temporalComponent;
    }

    public String getBlockHosts() {
        return blockHosts;
    }

    public void setBlockHosts(String blockHosts) {
        this.blockHosts = blockHosts;
    }

    public long getByteOffset() {
        return byteOffset;
    }

    public void setByteOffset(long byteOffset) {
        this.byteOffset = byteOffset;
    }

    public long getByteLength() {
        return byteLength;
    }

    public void setByteLength(long byteLength) {
        this.byteLength = byteLength;
    }

    public int getCompressionCode() {
        return compressionCode;
    }

    public void setCompressionCode(int compressionCode) {
        this.compressionCode = compressionCode;
    }

  public String getCorner() {
    return this.corner;
  }

  public void setCorner(String corner) {
    this.corner = corner;
  }


  public void write(Kryo kryo, Output output) {

  }

  public void read(Kryo kryo, Input input) {

  }

  public void write(DataOutput dataOutput) throws IOException {

  }

  public void readFields(DataInput dataInput) throws IOException {

  }
}
