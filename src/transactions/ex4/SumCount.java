package transactions.ex4;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCount implements Writable {
    private double total;
    private long count;

    public SumCount(double total, long count) {
        this.total = total;
        this.count = count;
    }

    public SumCount() {
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(getTotal());
        dataOutput.writeLong(getCount());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setTotal(dataInput.readDouble());
        setCount(dataInput.readLong());
    }
}
