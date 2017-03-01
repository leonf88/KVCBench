package pegasus.pegasus;

public class BlockElem<T> {
    public short row;
    public short col;
    public T val;

    public BlockElem(short in_row, short in_col, T in_val) {
        row = in_row;
        col = in_col;
        val = in_val;
    }
}
