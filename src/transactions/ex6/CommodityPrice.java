package transactions.ex6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommodityPrice implements Writable {

    private String comm;
    private double value;

    public CommodityPrice() {
    }

    public CommodityPrice(String comm, double value) {
        this.comm = comm;
        this.value = value;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getComm());
        dataOutput.writeDouble(getValue());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setComm(dataInput.readUTF());
        setValue(dataInput.readDouble());
    }
}
