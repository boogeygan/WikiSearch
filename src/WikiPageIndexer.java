package dom.gun.ire.minor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 *
 * @author gagan
 */
class MergeFileObject {
    boolean isNull;
    boolean isMin;
    BufferedReader in;
    String term;
    Integer df;
    int[] docIds;
    byte[] titles;
    byte[] contents;
    byte[] infoboxes;
    byte[] outlinks;
    byte[] categories;

    MergeFileObject() {
        term = "";
        df = 0;
        isNull = false;
        isMin = true;
    }
}

public class WikiPageIndexer {

    private static final int SPARSEVAL = 10000;
    private static final int MAXINDEXSIZE = 150000;
    private static final int MAXDF = 100000;
    private static final String TERTIARYINDEXFILENAME = "finalIndex/Output_Third";
    private static final String SECONDFILENAME = "finalIndex/Output_Second";
    String outputdirname;
    int countFileNum;
    TreeMap<String, TreeMap<Integer, TermAttributes>> map;
    HashSet<String> stopWords;

    public WikiPageIndexer(String outputdirname) {
        countFileNum = 0;
        this.outputdirname = outputdirname;
        map = new TreeMap<String, TreeMap<Integer, TermAttributes>>();
        fetchStopWords();
    }

    private boolean checkFP(MergeFileObject mfo[]) {

        for (MergeFileObject x : mfo) {
            if (!x.isNull) {
                return true;
            }
        }

        return false;
    }

    public void MergeFiles() {
        File dir = new File(outputdirname + "temp/");
        File files[] = dir.listFiles();

        TreeMap<String, ArrayList<Integer>> localmap = null;

        int x;

        MergeFileObject[] mfo = new MergeFileObject[files.length];
        BufferedWriter outMain = null;
        BufferedWriter outSecond = null;

        try {
            outMain = new BufferedWriter(new FileWriter(outputdirname + "finalIndex/Output_Main"));
            outSecond = new BufferedWriter(new FileWriter(outputdirname + "finalIndex/Output_Second"));
            long mainOffset = 0;
            long tempOffset = 0;

            for (int i = 0; i < mfo.length; i++) {
                mfo[i] = new MergeFileObject();
                mfo[i].in = new BufferedReader(new FileReader(files[i]));
            }

            while (checkFP(mfo)) {

                localmap = new TreeMap<String, ArrayList<Integer>>();

                // fetching each lowest term from each file and inserting into the treemap with indexes
                for (int i = 0; i < mfo.length; i++) {

                    //System.outMain.println(mfo[i].isMin);

                    if (!mfo[i].isNull) {

                        if (mfo[i].isMin) {

                            x = mfo[i].in.read();

                            if (x == -1) {
                                mfo[i].isNull = true;
                                continue;
                            }

                            mfo[i].term = "";

                            while (x != ':') {
                                mfo[i].term += (char) x;
                                x = mfo[i].in.read();
                            }

                            String df = "";
                            int iDf = mfo[i].in.read();

                            while (iDf != ':') {
                                df += (char) iDf;
                                iDf = mfo[i].in.read();
                            }

                            mfo[i].df = Integer.parseInt(df, 16);

                            mfo[i].docIds = new int[mfo[i].df];
                            mfo[i].titles = new byte[mfo[i].df];
                            mfo[i].categories = new byte[mfo[i].df];
                            mfo[i].infoboxes = new byte[mfo[i].df];
                            mfo[i].outlinks = new byte[mfo[i].df];
                            mfo[i].contents = new byte[mfo[i].df];

                            for (int j = 0; j < mfo[i].df; j++) {

                                String id = "";
                                int docId = mfo[i].in.read();

                                while (docId != ':') {
                                    id += (char) docId;
                                    docId = mfo[i].in.read();
                                }

                                mfo[i].docIds[j] = Integer.parseInt(id, 16);


                                x = mfo[i].in.read();

                                while (x != ':') {

                                    switch (x) {
                                        case 'T':
                                            mfo[i].titles[j] = (byte) mfo[i].in.read();
                                            break;
                                        case 'C':
                                            mfo[i].contents[j] = (byte) mfo[i].in.read();
                                            break;
                                        case 'I':
                                            mfo[i].infoboxes[j] = (byte) mfo[i].in.read();
                                            break;
                                        case 'O':
                                            mfo[i].outlinks[j] = (byte) mfo[i].in.read();
                                            break;
                                        case 'G':
                                            mfo[i].categories[j] = (byte) mfo[i].in.read();
                                            break;
                                    }

                                    x = mfo[i].in.read();
                                }
                            }
                        }

                        ArrayList<Integer> doclist;
                        doclist = localmap.get(mfo[i].term);

                        if (doclist == null) {
                            doclist = new ArrayList<Integer>();
                            doclist.add(i);
                            localmap.put(mfo[i].term, doclist);
                        } else {
                            doclist.add(i);
                        }
                    }
                }

                // initialise isMin
                for (int i = 0; i < mfo.length; i++) {
                    mfo[i].isMin = false;
                }

                if (localmap.size() != 0) {

                    String minTerm = localmap.firstKey();
                    ArrayList<Integer> minIndex = localmap.get(minTerm);

                    int df = 0;

                    for (int j : minIndex) {
                        df += mfo[j].df;
                    }

                    String s;
                    int countDF = 0;

/*
                    if (df > MAXDF) {
                        System.out.print(minTerm + " ## ");
                        System.out.println(df);
                    }
*/
                    // sort the docid's for the object that are currently
                    // going to be written

                    TreeMap<Integer, FileObject> docidlist = sortDocIDforMFO(df, minIndex, mfo);

                    Set<Map.Entry<Integer, FileObject>> set = docidlist.entrySet();

                    int olddocid = 0;

                    for (Map.Entry<Integer, FileObject> currid : set) {

                        int docid = currid.getKey();
                        FileObject fo = currid.getValue();

                        if (df > MAXDF) {
                            if (fo.content <= 2 && (fo.title + fo.infobox + fo.categories + fo.outlinks) <= 0) {
                                countDF++;
                                continue;
                            }
                        }

                        s = Integer.toHexString(docid - olddocid) + ":";
                        outMain.write(s);
                        olddocid = docid;
                        tempOffset += s.length();

                        if (fo.title != 0) {
                            outMain.write('T');
                            String temp = Integer.toHexString(fo.title);
                            outMain.write(temp);
                            outMain.write(":");
                            tempOffset += temp.length() + 2;
                        }

                        if (fo.content != 0) {
                            outMain.write('C');
                            String temp = Integer.toHexString(fo.content);
                            outMain.write(temp);
                            outMain.write(":");
                            tempOffset += temp.length() + 2;
                        }

                        if (fo.categories != 0) {
                            outMain.write('G');
                            String temp = Integer.toHexString(fo.categories);
                            outMain.write(temp);
                            outMain.write(":");
                            tempOffset += temp.length() + 2;
                        }

                        if (fo.infobox != 0) {
                            outMain.write('I');
                            String temp = Integer.toHexString(fo.infobox);
                            outMain.write(temp);
                            outMain.write(":");
                            tempOffset += temp.length() + 2;
                        }

                        if (fo.outlinks != 0) {
                            outMain.write('O');
                            String temp = Integer.toHexString(fo.outlinks);
                            outMain.write(temp);
                            outMain.write(":");
                            tempOffset += temp.length() + 2;
                        }

                        outMain.write('#');
                        tempOffset++;
                    }
                    outMain.write('\n');
                    tempOffset++;

                    outSecond.write(minTerm);

                    outSecond.write(':');
                    outSecond.write(Long.toHexString(mainOffset) + ":");
                    mainOffset = tempOffset;

                    s = Integer.toHexString(df - countDF) + "\n";
                    outSecond.write(s);
                }
            }

            for (File deletecandidate : files) {
                deletecandidate.delete();
            }

        } catch (FileNotFoundException ex) {
            System.err.println(ex);
        } catch (IOException ex) {
            System.err.println(ex);
        } finally {
            try {
                outMain.close();
                outSecond.close();
                for (MergeFileObject m : mfo) {
                    m.in.close();
                }
            } catch (IOException ex) {
                System.err.println(ex);
            }
        }
    }

    private TreeMap<Integer, FileObject> sortDocIDforMFO(int df, ArrayList<Integer> minIndex, MergeFileObject[] mfo) {

        TreeMap<Integer, FileObject> docidlist = new TreeMap<Integer, FileObject>();

        FileObject fo;

        for (int i : minIndex) {
            mfo[i].isMin = true;

            for (int j = 0; j < mfo[i].docIds.length; j++) {

                fo = new FileObject();

                fo.title = mfo[i].titles[j];
                fo.content = mfo[i].contents[j];
                fo.infobox = mfo[i].infoboxes[j];
                fo.categories = mfo[i].categories[j];
                fo.outlinks = mfo[i].outlinks[j];

                fo.docId = mfo[i].docIds[j];

                docidlist.put(fo.docId, fo);
            }
        }

        return docidlist;
    }

    // can't be overridden
    public final void fetchStopWords() {

        FileReader fin = null;
        BufferedReader bf = null;
        stopWords = new HashSet<String>();

        try {
            fin = new FileReader("stopWordList");

            if (fin != null) {
                bf = new BufferedReader(fin);
            }

            String word;

            do {
                word = bf.readLine();

                if (word != null) {
                    stopWords.add(word);
                }

            } while (word != null);

            bf.close();
            fin.close();

        } catch (FileNotFoundException ex) {
            System.out.println("'stopWordList' File Not Found");
        } catch (IOException ex) {
            System.out.println("IO Error");
        }
    }

    public boolean containsColon(String str) {
        if (str.contains(":")) {
            return true;
        } else {
            return false;
        }
    }

    public void indexPage(WikiPage page) {

        Stemmer stem = new Stemmer();
        StringTokenizer st;
        int id = page.getId();
        String str;
        // index titles
        st = new StringTokenizer(page.getTitle().toString(), ",`~!@#$%^&*()-_=+{[}]|\\:;\"\'<>.?/0123456789\n \t");

        while (st.hasMoreTokens()) {

            str = st.nextToken().toLowerCase();

            if (!stopWords.contains(str)) {
                str = stem.stemIt(str);
                addTerm(str, id, FieldType.title);
                //System.out.println("This is the tag type: "+FieldType.title);
            }
        }
//System.exit(1);
        // index content
        st = new StringTokenizer(page.getContent().toString(), ",`~!@#$%^&*()-_=+{[}]|\\:;\"\'<>.?/0123456789\n \t");

        while (st.hasMoreTokens()) {
            str = st.nextToken().toLowerCase();

            if (!stopWords.contains(str)) {
                str = stem.stemIt(str);
                addTerm(str, id, FieldType.content);
            }
        }


        // index Infobox		
        st = new StringTokenizer(page.getInfobox().toString(), ",`~!@#$%^&*()-_=+{[}]|\\:;\"\'<>.?/0123456789\n \t");

        while (st.hasMoreTokens()) {
            str = st.nextToken().toLowerCase();

            if (!stopWords.contains(str)) {
                str = stem.stemIt(str);
                addTerm(str, id, FieldType.infobox);
            }
        }

        // index Categories
        st = new StringTokenizer(page.getCategories().toString(), ",`~!@#$%^&*()-_=+{[}]|\\:;\"\'<>.?/0123456789\n \t");

        while (st.hasMoreTokens()) {
            str = st.nextToken().toLowerCase();

            if (!stopWords.contains(str)) {
                str = stem.stemIt(str);
                addTerm(str, id, FieldType.categories);

            }
        }


        // index outlinks
        st = new StringTokenizer(page.getOutlinks().toString(), ",`~!@#$%^&*()-_=+{[}]|\\:;\"\'<>.?/0123456789\n \t");

        while (st.hasMoreTokens()) {
            str = st.nextToken().toLowerCase();

            if (!stopWords.contains(str)) {
                str = stem.stemIt(str);
                addTerm(str, id, FieldType.outlinks);
            }
        }

        if (map.size() > MAXINDEXSIZE) {
            String filename = outputdirname + "temp/Output_" + countFileNum;
            System.out.println("Temp File Count: "+countFileNum);
            this.writeToFile(filename);
            countFileNum++;
            map = null;
            map = new TreeMap<String, TreeMap<Integer, TermAttributes>>();
            
        }
    }

    void readFromFile(String filename) {

        FileReader fin = null;
        BufferedReader in = null;

        try {
            fin = new FileReader(filename);
            in = new BufferedReader(fin);

            String term = "";

            int x = in.read();

            FileObject fo = new FileObject();

            while (x != -1) {

                while (x != ':') {
                    term += (char) x;
                    x = in.read();
                }

                System.out.println(term);

                String df = "";

                int iDf = in.read();

                while (iDf != ':') {
                    df += (char) iDf;
                    iDf = in.read();
                }

                iDf = Integer.parseInt(df);
                System.out.print(df + " ");

                for (int i = 0; i < iDf; i++) {

                    fo.makezero();
                    String id = "";
                    fo.docId = in.read();

                    while (fo.docId != ':') {
                        id += (char) fo.docId;
                        fo.docId = in.read();
                    }

                    fo.docId = Integer.parseInt(id);


                    x = in.read();


                    while (x != ':') {

                        switch (x) {
                            case 'T':
                                fo.title = (byte) in.read();
                                break;
                            case 'C':
                                fo.content = (byte) in.read();
                                break;
                            case 'I':
                                fo.infobox = (byte) in.read();
                                break;
                            case 'O':
                                fo.outlinks = (byte) in.read();
                                break;
                            case 'G':
                                fo.categories = (byte) in.read();
                                break;
                        }
                        x = in.read();
                    }

                }

                x = in.read();
                term = "";
            }
        } catch (OptionalDataException ex) {
            System.err.println(ex);
        } catch (FileNotFoundException ex) {
            System.err.println(ex);
        } catch (IOException ex) {
            System.err.println(ex);
        } finally {
            try {
                in.close();
                fin.close();
            } catch (IOException ex) {
                System.err.println(ex);
            }
        }

    }

    void writeString(String term, OutputStream out)
            throws IOException {
        if (term != null && out != null) {
            for (int i = 0; i < term.length(); i++) {
                if (term.charAt(i) != ':') {
                    out.write(term.charAt(i));
                }
            }
        }
    }

    void writeToFile(String filename) {
        FileWriter fout = null;
        BufferedWriter out = null;

        try {
            fout = new FileWriter(filename);
            out = new BufferedWriter(fout);

            Set<Map.Entry<String, TreeMap<Integer, TermAttributes>>> setTerms = map.entrySet();

            TreeMap<Integer, TermAttributes> docs;
            TermAttributes ta;

            for (Map.Entry<String, TreeMap<Integer, TermAttributes>> curr : setTerms) {
                String key = curr.getKey();
                docs = curr.getValue();
                out.write(key);
                out.write(':');
                key = "" + Integer.toHexString(docs.size());
                out.write(key);
                out.write(':');

                Set<Map.Entry<Integer, TermAttributes>> setDocs = docs.entrySet();

                for (Map.Entry<Integer, TermAttributes> currDoc : setDocs) {
                    int docid = currDoc.getKey();
                    ta = currDoc.getValue();

                    key = Integer.toHexString(docid);
                    out.write(key);
                    out.write(':');

                    if (ta.getTitle() != 0) {
                        out.write('T');
                        out.write(ta.getTitle());
                    }
                    if (ta.getContent() != 0) {
                        out.write('C');
                        out.write(ta.getContent());
                    }
                    if (ta.getCategories() != 0) {
                        out.write('G');
                        out.write(ta.getCategories());
                    }
                    if (ta.getInfobox() != 0) {
                        out.write('I');
                        out.write(ta.getInfobox());
                    }
                    if (ta.getOutlinks() != 0) {
                        out.write('O');
                        out.write(ta.getOutlinks());
                    }
                    out.write(':');
                }
            }
        } catch (FileNotFoundException ex) {
            System.err.println(ex);
        } catch (IOException ex) {
            System.err.println(ex);
        } finally {
            try {
                //out.writeObject(null);
                out.close();
                //bout.close();
                fout.close();
            } catch (IOException ex) {
                System.err.println(ex);
            }
        }
    }

    private byte[] intToByteArray(int value) {
        return new byte[]{
                    (byte) (value >>> 24),
                    (byte) (value >>> 16),
                    (byte) (value >>> 8),
                    (byte) value};
    }

    private int byteArrayToInt(byte[] b) {
        return (b[0] << 24)
                + ((b[1] & 0xFF) << 16)
                + ((b[2] & 0xFF) << 8)
                + (b[3] & 0xFF);
    }

/*    public void display() {
        Set<Map.Entry<String, TreeMap<Integer, TermAttributes>>> setTerms = map.entrySet();

        TreeMap<Integer, TermAttributes> docs;
        TermAttributes ta;

        for (Map.Entry<String, TreeMap<Integer, TermAttributes>> curr : setTerms) {
            System.out.println(curr.getKey() + " ");
            docs = curr.getValue();

            Set<Map.Entry<Integer, TermAttributes>> setDocs = docs.entrySet();

            for (Map.Entry<Integer, TermAttributes> currDoc : setDocs) {
                int docid = currDoc.getKey();
                ta = currDoc.getValue();

                System.out.println(docid + " ");
                System.out.println(ta.getTitle() + " ");
            }

        }
    }
*/
    private void addTerm(String term, int id, FieldType tag) {

        TreeMap<Integer, TermAttributes> tm = map.get(term);

        if (tm != null) {
            TermAttributes ta = tm.get(id);

            int x;

            if (ta != null) {
                switch (tag) {
                    case title:
                        ta.increaseTitleCount();
                        break;
                    case content:
                        ta.increaseContentCount();
                        break;
                    case infobox:
                        ta.increaseInfoboxCount();
                        break;
                    case outlinks:
                        ta.increaseOutlinksCount();
                        break;
                    case categories:
                        ta.increaseCategoriesCount();
                        break;
                }
            } else {
                ta = new TermAttributes();
                ta.setId(id);

                switch (tag) {
                    case title:
                        ta.increaseTitleCount();
                        break;
                    case content:
                        ta.increaseContentCount();
                        break;
                    case infobox:
                        ta.increaseInfoboxCount();
                        break;
                    case outlinks:
                        ta.increaseOutlinksCount();
                        break;
                    case categories:
                        ta.increaseCategoriesCount();
                        break;
                }

                tm.put(id, ta);
            }
        } else {
            tm = new TreeMap<Integer, TermAttributes>();
            TermAttributes ta = new TermAttributes();

            ta.setId(id);

            switch (tag) {
                case title:
                    ta.increaseTitleCount();
                    break;
                case content:
                    ta.increaseContentCount();
                    break;
                case infobox:
                    ta.increaseInfoboxCount();
                    break;
                case outlinks:
                    ta.increaseOutlinksCount();
                    break;
                case categories:
                    ta.increaseCategoriesCount();
                    break;
            }

            tm.put(id, ta);
            map.put(term, tm);
        }
    }

    void createTertiary() {
        String as[];
        String s;
        BufferedReader in = null;
        BufferedWriter out = null;

        try {
            in = new BufferedReader(new FileReader(outputdirname + SECONDFILENAME));
            out = new BufferedWriter(new FileWriter(outputdirname + TERTIARYINDEXFILENAME));

            s = in.readLine();
            int count = 0;
            int i = 0;

            while (s != null) {
                if (count >= (SPARSEVAL * i)) {
                    as = s.split(":");
                    i++;

                    out.write(as[0] + ":" + count);
                    out.newLine();
                }

                count += s.length() + 1;
                s = in.readLine();
            }
        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            try {
                out.close();
                in.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }
    }
}
