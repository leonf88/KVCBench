package mlbench.kmeans;

import mpid.core.MPI_D_Comparator;

public class PointWritableComparator implements MPI_D_Comparator<PointWriable> {

    @Override
    public int compare(PointWriable o1, PointWriable o2) {
        for (int i = 0; i < o1.values.length; i++) {
            if (o1.values[i] < o2.values[i]) {
                return -1;
            } else if (o1.values[i] > o2.values[i]) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
            int a = (b1[i] & 0xff);
            int b = (b2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return l1 - l2;
    }
}
