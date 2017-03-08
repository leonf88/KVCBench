package mlbench.pagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageWritable implements Writable {
    int url;
    double val;
    List<Integer> links;

    public PageWritable() {

    }

    public PageWritable(int url) {
        this.url = url;
        this.val = 1;
    }

    private PageWritable(PageWritable p) {
        this.url = p.url;
        this.val = p.val;
        this.links = new ArrayList<>(p.links);
    }

    public void addLink(int url) {
        if (links == null) {
            links = new ArrayList<>();
        }
        links.add(url);
    }

    public void addLinks(List<Integer> urls) {
        if (links == null) {
            links = new ArrayList<>(urls);
        } else {
            links.addAll(urls);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(url);
        out.writeDouble(val);
        out.writeInt(links.size());
        for (Integer lk : links) {
            out.writeInt(lk);
        }
    }

    public PageWritable clone() {
        return new PageWritable(this);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url = in.readInt();
        val = in.readDouble();
        int s = in.readInt();
        links = new ArrayList<>(s);
        for (int i = 0; i < s; i++) {
            links.add(in.readInt());
        }
    }

    @Override
    public String toString() {
        return "URL: " + url + " value: " + val + "; links: "
                + Arrays.toString(links.toArray());
    }
}
