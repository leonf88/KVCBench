package pegasus.hadi;

// Flajolet - Martin bitmask(bitstring) class
public class FMBitmask {
    // generate K replicated bitmasks for one node
    public static String generate_bitmask(int number_node, int K, int encode_bitmask) {
        int i;
        int size_bitmask = 32;
        String bitmask = "bsi0:0:1";
        int bm_array[] = new int[K];

        for (i = 0; i < K; i++) {
            if (encode_bitmask == 1)
                bm_array[i] = create_random_bm(number_node, size_bitmask);
            else
                bitmask = bitmask + " " + Integer.toHexString(create_random_bm(number_node,
                        size_bitmask));
        }

        if (encode_bitmask == 1) {
            String encoded_bitmask = BitShuffleCoder.encode_bitmasks(bm_array, K);

            bitmask += (" " + encoded_bitmask);
        }

        return bitmask;
    }


    // Create a Flajolet-Martin bitstring. The maximum number of nodes is currently 4 billion.
    public static int create_random_bm(int number_node, int size_bitmask) {
        int j;

        // cur_random is between 0 and 1.
        double cur_random = Math.random();

        double threshold = 0;
        for (j = 0; j < size_bitmask - 1; j++) {
            threshold += Math.pow(2, -1 * j - 1);

            if (cur_random < threshold) {
                break;
            }
        }

        int bitmask = 0;

        if (j < size_bitmask - 1) {
            int small_bitmask = 1 << (size_bitmask - 1 - j);
            // move small_bitmask to MSB bits of bitmask;
            bitmask = small_bitmask << (32 - size_bitmask);
        }

        return bitmask;
    }

    // calculate Neighborhood function N(h) from bitmasks.
    public static double nh_from_bitmask(long[] bitmask, int K) {
        int i;
        double avg_bitpos = 0;

        for (i = 0; i < K; i++) {
            avg_bitpos += (double) FMBitmask.find_least_zero_pos(bitmask[i]);
        }

        avg_bitpos = avg_bitpos / (double) K;

        return Math.pow(2, avg_bitpos) / 0.77351;
    }

    // fine the least zero bit position in a number
    public static int find_least_zero_pos(long number) {
        int i;

        for (i = 0; i < 32; i++) {
            int mask = 1 << (31 - i);

            if ((number & mask) == 0) {
                return i;
            }
        }

        return i;
    }
}
