package transactions.ex3.beans;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommFlowType implements WritableComparable<CommFlowType> {

    private String comm;
    private String flowtype;

    public CommFlowType() {
    }

    public CommFlowType(String comm, String flowtype) {
        this.comm = comm;
        this.flowtype = flowtype;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommFlowType that = (CommFlowType) o;
        return Objects.equals(comm, that.comm) && Objects.equals(flowtype, that.flowtype);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comm, flowtype);
    }

    @Override
    public int compareTo(CommFlowType o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.comm);
        dataOutput.writeUTF(this.flowtype);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.comm = dataInput.readUTF();
        this.flowtype = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return comm + "\t" + flowtype;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    public String getFlowtype() {
        return flowtype;
    }

    public void setFlowtype(String flowtype) {
        this.flowtype = flowtype;
    }
}
