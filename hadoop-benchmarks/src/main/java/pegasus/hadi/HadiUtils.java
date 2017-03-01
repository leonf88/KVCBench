package pegasus.hadi;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;

public class HadiUtils {
    // record the latest radius info, and
    // delete previous radius info if not useful for effective radius calculation.
    public static String update_radhistory(long[] self_bitmask, String saved_rad_nh, int
            cur_radius, int nreplication) {
        double max_nh = FMBitmask.nh_from_bitmask(self_bitmask, nreplication);
        double ninety_maxnh = 0.9 * max_nh;
        String[] token = saved_rad_nh.split(":");
        int i;
        String result = "";
        boolean bAboveThreshold = false;
        int cur_hop = 0, prev_hop = 0;
        double cur_nh, prev_nh = 0;
        DecimalFormat df = new DecimalFormat("#.#");
        boolean bFirstAdd = true;

        for (i = 0; i < token.length; i += 2) {
            cur_hop = Integer.parseInt(token[i]);
            cur_nh = Double.parseDouble(token[i + 1]);
            if (bAboveThreshold == false) {
                if (cur_nh >= ninety_maxnh) {
                    bAboveThreshold = true;

                    if (i > 0) {
                        result = result + ":" + prev_hop + ":" + prev_nh;
                    }
                }
            }

            if (bAboveThreshold) {
                result = result + ":" + cur_hop + ":" + df.format(cur_nh);
            }
            prev_nh = cur_nh;
            prev_hop = cur_hop;
        }

        if (token.length > 0 && result.length() == 0 && cur_hop > 0) {
            result = result + ":" + prev_hop + ":" + prev_nh;
        }

        result = result + ":" + cur_radius + ":" + df.format(max_nh);
        //System.out.println("[DEBUG] update_radhistory result=" + result);

        return result;
    }

    // calculate the effective diameter of a graph, given neighborhood results.
    public static float effective_diameter(float[] N, int max_radius) {
        float max_nh = N[max_radius];
        int i;
        float threshold = max_nh * 0.9f;

        for (i = 1; i <= max_radius; i++) {
            if (N[i] >= threshold) {
                float decimal = (threshold - N[i - 1]) / (N[i] - N[i - 1]);
                return (i - 1 + decimal);
            }
        }

        return -1;
    }

    // calculate the average diameter of a graph, given neighborhood results.
    public static float average_diameter(float[] N, int max_radius) {
        float min_nh = N[0];
        float max_nh = N[max_radius];
        int h;
        float sum = 0;

        for (h = 1; h <= max_radius; h++) {
            sum += h * (N[h] - N[h - 1]);
        }

        sum = sum / (max_nh - min_nh);

        return sum;
    }

    // read neighborhood number after each iteration.
    public static HadiResultInfo readNhoodOutput(String new_path) throws Exception {
        String output_path = new_path + "/part-00000";
        String str = "";
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(output_path), "UTF8"));
            str = in.readLine();
        } catch (UnsupportedEncodingException e) {
        } catch (IOException e) {
        }

        final String[] line = str.split("\t");

        HadiResultInfo ri = new HadiResultInfo();
        ri.nh = Float.parseFloat(line[1]);
        ri.converged_nodes = Integer.parseInt(line[2]);
        ri.changed_nodes = Integer.parseInt(line[3]);

        return ri;
    }
}

