package core.adapt.iterator;

/**
 * Created by qui on 5/20/15.
 */

import core.common.key.RawIndexKey;

import java.io.Serializable;

/**
 * An wrapper class over CartilageIndexKey (to reuse much of the functionality)
 *
 * @author alekh
 */
public class IteratorRecord extends RawIndexKey implements Serializable {
    private static final long serialVersionUID = 1L;

    public IteratorRecord() {
        super('|');
    }

    public IteratorRecord(char delimiter) {
        super(delimiter);
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public int getOffset() {
        return this.offset;
    }

    public int getLength() {
        return this.length;
    }


}
