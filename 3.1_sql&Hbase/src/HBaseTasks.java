import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBaseTasks {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = System.getenv("PRIVATE_IP");
    /**
     * The name of your HBase table.
     */
    private static String tableName = "songdata";
    /**
     * HTable handler.
     */
    private static HTableInterface songsTable;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Initialize HBase connection.
     * 
     * @throws IOException
     */
    private static void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        LOGGER.setLevel(Level.ERROR);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = HConnectionManager.createConnection(conf);
        songsTable = conn.getTable(Bytes.toBytes(tableName));
    }

    /**
     * Clean up resources.
     * 
     * @throws IOException
     */
    private static void cleanup() throws IOException {
        if (songsTable != null) {
            songsTable.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * You should complete the missing parts in the following method. Feel free
     * to add helper functions if necessary.
     *
     * For all questions, output your answer in ONE single line, i.e. use
     * System.out.print().
     *
     * @param args
     *            The arguments for main method.
     */
    public static void main(String[] args) throws IOException {
        initializeConnection();
        switch (args[0]) {
        case "demo":
            demo();
            break;
        case "q12":
            q12();
            break;
        case "q13":
            q13();
            break;
        case "q14":
            q14();
            break;
        case "q15":
            q15();
            break;
        case "q16":
            q16();
        }
        cleanup();
    }

    /**
     * This is a demo of how to use HBase Java API. It will print all the
     * artist_names starting with "The Beatles".
     * 
     * @throws IOException
     */
    private static void demo() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^The Beatles.*");
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }

    /**
     * Question 12.
     *
     * What was that song whose name started with "Total" and ended with
     * "Water"? Write an HBase query that finds the track that the person is
     * looking for. The title starts with "Total" and ends with "Water", both
     * are case sensitive. Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q12() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^Total.*Water$");
        Filter filter = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        scan.setFilter(filter);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
//        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();

    }

    /**
     * Question 13.
     *
     * I don't remember the exact title, it was that song by "Kanye West", and
     * the title started with either "Apologies" or "Confessions". Not sure
     * which... Write an HBase query that finds the track that the person is
     * looking for. The artist_name contains "Kanye West", and the title starts
     * with either "Apologies" or "Confessions" (Case sensitive). Print the
     * track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q13() throws IOException {
        Scan scan = new Scan();
        byte[] bCol1 = Bytes.toBytes("artist_name");
        byte[] bCol2 = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);

        RegexStringComparator comp2_1 = new RegexStringComparator("^Apologies.*");
        RegexStringComparator comp2_2 = new RegexStringComparator("^Confessions.*");
        SubstringComparator comp1 = new SubstringComparator("Kanye West");
        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.EQUAL, comp1);
        Filter filter2_1 = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp2_1);
        Filter filter2_2 = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp2_2);
        FilterList subFilterList1 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1);
        FilterList subFilterList2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, filter2_1, filter2_2);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, subFilterList1, subFilterList2);
        scan.setFilter(filterList);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol2)));
        }
//        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();

    }

    /**
     * Question 14.
     *
     * There was that new track by "Bob Marley" that was really long. Do you
     * know? Write an HBase query that finds the track the person is looking
     * for. The artist_name has a prefix of "Bob Marley", duration no less than
     * 400, and year 2000 and onwards (Case sensitive). Print the track title(s)
     * in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q14() throws IOException {

        /*
         * class doubleComparator extends WritableByteArrayComparable { public
         * doubleComparator(byte[] value) { super(value); }
         * 
         * public int compareTo(byte[] buffer) { double arg1 =
         * this.byteToDouble(this.getValue()); double arg2 =
         * this.byteToDouble(buffer); int cmp = 0; if (arg1 > arg2) cmp = 1;
         * else if (arg1 < arg2) cmp = -1; else cmp = 0; return cmp; // Bring
         * WritableComparator code local
         * 
         * }
         * 
         * public int compareTo(byte[] buffer, int offset1, int offset2) { //
         * Bring WritableComparator code local return (this.compareTo(buffer));
         * }
         * 
         * public double byteToDouble(byte[] b) { long l;
         * 
         * l = b[0]; l &= 0xff; l |= ((long) b[1] << 8); l &= 0xffff; l |=
         * ((long) b[2] << 16); l &= 0xffffff; l |= ((long) b[3] << 24); l &=
         * 0xffffffffl; l |= ((long) b[4] << 32); l &= 0xffffffffffl;
         * 
         * l |= ((long) b[5] << 40); l &= 0xffffffffffffl; l |= ((long) b[6] <<
         * 48);
         * 
         * l |= ((long) b[7] << 56); return Double.longBitsToDouble(l); } }
         */

        Scan scan = new Scan();
        byte[] bCol1 = Bytes.toBytes("artist_name");
        byte[] bCol2 = Bytes.toBytes("duration");
        byte[] bCol3 = Bytes.toBytes("title");
        byte[] bCol4 = Bytes.toBytes("year");

        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);
        scan.addColumn(bColFamily, bCol3);
        scan.addColumn(bColFamily, bCol4);
        

        RegexStringComparator comp1 = new RegexStringComparator("^Bob Marley.*");
        RegexStringComparator comp2 = new RegexStringComparator("(\\d{2,9}|[4-9])\\d{2}\\.\\d+");
        RegexStringComparator comp3 = new RegexStringComparator("2000");
        RegexStringComparator comp4 = new RegexStringComparator("1\\d{3}");
        RegexStringComparator comp5 = new RegexStringComparator("0");


        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.EQUAL, comp1);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp2);
        Filter filter3 = new SingleColumnValueFilter(bColFamily, bCol4, CompareFilter.CompareOp.EQUAL, comp3);
        Filter filter4 = new SingleColumnValueFilter(bColFamily, bCol4, CompareFilter.CompareOp.EQUAL, comp4);
        Filter filter5 = new SingleColumnValueFilter(bColFamily, bCol4, CompareFilter.CompareOp.EQUAL, comp5);


        FilterList subFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filter3, filter4, filter5);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2);
        scan.setFilter(filterList);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.print(Bytes.toString(r.getValue(bColFamily, bCol3))+" ");
//            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol3))+" "+Bytes.toString(r.getValue(bColFamily, bCol4)));

        }
//        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }

    /**
     * Question 15.
     *
     * I heard a really great song about "Family" by this really cute singer, I
     * think his name was "Consequence" or something... Write an HBase query
     * that finds the track the person is looking for. The track has an
     * artist_hotttnesss of at least 1, and the artist_name contains
     * "Consequence". Also, the title contains "Family" (Case sensitive). Print
     * the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q15() throws IOException {

        Scan scan = new Scan();
        byte[] bCol1 = Bytes.toBytes("artist_name");
        byte[] bCol2 = Bytes.toBytes("artist_hotttnesss");
        byte[] bCol3 = Bytes.toBytes("title");
        
        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);
        scan.addColumn(bColFamily, bCol3);
        org.apache.hadoop.hbase.filter.

                SubstringComparator comp1 = new SubstringComparator("Consequence");
        RegexStringComparator comp2 = new RegexStringComparator("(\\d{2,9}|[1-9])(\\.\\d+)");
        SubstringComparator comp3 = new SubstringComparator("Family");

        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.EQUAL, comp1);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.EQUAL, comp2);
        Filter filter3 = new SingleColumnValueFilter(bColFamily, bCol3, CompareFilter.CompareOp.EQUAL, comp3);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2, filter3);
        scan.setFilter(filterList);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol3)));
//            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol3)) + " " + Bytes.toString(r.getValue(bColFamily, bCol2)));
        }
//        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();

    }

    /**
     * Question 16.
     *
     * Hey what was that "Love" song that "Gwen Guthrie" came out with in 1990?
     * No, no, it wasn't the sad one, nothing "Bitter" or "Never"... Write an
     * HBase query that finds the track the person is looking for. The track has
     * an artist_name prefix of "Gwen Guthrie", the title contains "Love" but
     * does NOT contain "Bitter" or "Never", the year equals to 1990. Print the
     * track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter
     * list and/or return type.
     */
    private static void q16() throws IOException {
        Scan scan = new Scan();
        byte[] bCol1 = Bytes.toBytes("artist_name");
        byte[] bCol2 = Bytes.toBytes("year");
        byte[] bCol3 = Bytes.toBytes("title");
        byte[] value = Bytes.toBytes(1990);
        scan.addColumn(bColFamily, bCol1);
        scan.addColumn(bColFamily, bCol2);
        scan.addColumn(bColFamily, bCol3);

        RegexStringComparator comp1 = new RegexStringComparator("^Gwen Guthrie.*");
        SubstringComparator comp2 = new SubstringComparator("Love");
        SubstringComparator comp3 = new SubstringComparator("Bitter");
        SubstringComparator comp4 = new SubstringComparator("Never");

        Filter filter1 = new SingleColumnValueFilter(bColFamily, bCol1, CompareFilter.CompareOp.EQUAL, comp1);
        Filter filter2 = new SingleColumnValueFilter(bColFamily, bCol2, CompareFilter.CompareOp.GREATER_OR_EQUAL,
                value);
        Filter filter3 = new SingleColumnValueFilter(bColFamily, bCol3, CompareFilter.CompareOp.NOT_EQUAL, comp3);
        Filter filter4 = new SingleColumnValueFilter(bColFamily, bCol3, CompareFilter.CompareOp.NOT_EQUAL, comp4);
        Filter filter5 = new SingleColumnValueFilter(bColFamily, bCol3, CompareFilter.CompareOp.EQUAL, comp2);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter1, filter2, filter3, filter4,
                filter5);
        scan.setFilter(filterList);
        // scan.setBatch(10);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol3)));
        }
//        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();

    }

}
