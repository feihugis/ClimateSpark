package gov.nasa.gsfc.cisto.cds.sia.core.io.key;

import org.apache.hadoop.fs.Path;


/**
 * The type Split json.
 */
public class SplitJson {

  private Path _path = null;
  private String[] _hosts = null;


    /**
     * Instantiates a new Split json.
     *
     * @param path  the path
     * @param hosts the hosts
     */
    public SplitJson(Path path, String[] hosts) {
    this._path = new Path(path.toString());
    this._hosts = new String[hosts.length];
    System.arraycopy(hosts, 0, this._hosts, 0, this._hosts.length);
  }


    /**
     * Gets path.
     *
     * @return the path
     */
    public Path getPath() {
    return this._path;
  }


    /**
     * Get hosts string [ ].
     *
     * @return the string [ ]
     */
    public String[] getHosts() {
    return this._hosts;
  }
}


/* Location:              /Users/mkbowen/Desktop/MerraTest.jar!/edu/gmu/stc/merra/hadoop/io/SplitJson.class
 * Java compiler version: 6 (50.0)
 * JD-Core Version:       0.7.1
 */