package gov.nasa.gsfc.cisto.cds.sia.core.variableentities;

import com.esotericsoftware.kryo.KryoSerializable;
import gov.nasa.gsfc.cisto.cds.sia.core.variablemetadata.SiaVariableAttribute;
import org.apache.hadoop.io.Writable;

//import com.sun.org.apache.xpath.internal.operations.String;
//import com.sun.org.apache.xpath.internal.operations.String;

/**
 * The interface Sia variable entity.
 */
public interface SiaVariableEntity extends KryoSerializable, Writable {
    /**
     * Sets var name.
     *
     * @param varName the var name
     */
    public void setVarName(String varName);

    /**
     * Gets var name.
     *
     * @return the var name
     */
    public String getVarName();

    /**
     * Initialize entity.
     *
     * @param siaVariableAttribute the sia variable attribute
     */
    public void initializeEntity(SiaVariableAttribute siaVariableAttribute);

    /**
     * Gets temporal component.
     *
     * @return the temporal component
     */
    public int getTemporalComponent();

    /**
     * Sets temporal component.
     *
     * @param temporalComponent the temporal component
     */
    public void setTemporalComponent(int temporalComponent);

    /**
     * Gets byte offset.
     *
     * @return the byte offset
     */
    public long getByteOffset();

    /**
     * Sets byte offset.
     *
     * @param byteOffset the byte offset
     */
    public void setByteOffset(long byteOffset);

    /**
     * Gets byte length.
     *
     * @return the byte length
     */
    public long getByteLength();

    /**
     * Sets byte length.
     *
     * @param byteLength the byte length
     */
    public void setByteLength(long byteLength);

    /**
     * Gets block hosts.
     *
     * @return the block hosts
     */
    public String getBlockHosts();

    /**
     * Sets block hosts.
     *
     * @param blockHosts the block hosts
     */
    public void setBlockHosts(String blockHosts);

    /**
     * Gets compression code.
     *
     * @return the compression code
     */
    public int getCompressionCode();

    /**
     * Sets compression code.
     *
     * @param compressionCode the compression code
     */
    public void setCompressionCode(int compressionCode);

    /**
     * Gets corner.
     *
     * @return the corner
     */
    public String getCorner();

    public void setCorner(String corner);
}
