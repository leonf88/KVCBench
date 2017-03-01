package pegasus.pegasus;

public class VectorElem<T> {
    public short row;
    public T val;

    public VectorElem(short in_row, T in_val) {
        row = in_row;
        val = in_val;
    }

    public double getDouble() {
        return ((Double) val).doubleValue();
    }
}
