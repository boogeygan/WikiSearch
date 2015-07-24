package dom.gun.ire.minor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 *
 * @author gagan
 */
public class Retriever implements Runnable {

    private HashSet<String> stopWords;

    class DenseElement {

        int df;
        int length;
        long offset;
        FieldType tt;
    }
    private static int SPARSEVAL = 10000;
    private int corpusSize;
    private String outputdirname;
    private TreeMap<String, Integer> tertiaryIndex;
    private TreeMap<Integer, Integer> forwardIndex;
    private TreeMap<Integer, Integer> docLength;
    ArrayList<ArrayList<FileObject>> docListArray;
    ArrayList<DenseElement> de;

    Retriever(String outputdirname) {
        this.outputdirname = outputdirname;
        fetchTertiaryIndex();
        fetchStopWords();
        fetchForwardIndex();
    }

    private void fetchTertiaryIndex() {
        String as[];
        String s;
        BufferedReader in = null;
        try {
            tertiaryIndex = new TreeMap<String, Integer>();
            in = new BufferedReader(new FileReader(outputdirname + "finalIndex/Output_Third"));
            s = in.readLine();

            while (s != null) {
                as = s.split(":");
                tertiaryIndex.put(as[0], Integer.parseInt(as[1]));

                s = in.readLine();
            }
        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }
    }

    private void fetchForwardIndex() {
        String as[];
        String s;
        BufferedReader in = null;

        try {
            forwardIndex = new TreeMap<Integer, Integer>();
            docLength = new TreeMap<Integer, Integer>();
            in = new BufferedReader(new FileReader(outputdirname + "finalIndex/Output_Forward"));

            s = in.readLine();
            int count = 0;
            int i = 0;

            while (s != null) {
                as = s.split(":");

                docLength.put(Integer.parseInt(as[0], 16), Integer.parseInt(as[1], 16));
                if (count >= (SPARSEVAL * i)) {
                    i++;
                    forwardIndex.put(Integer.parseInt(as[0], 16), count);
                }

                count += s.length() + 1;
                s = in.readLine();
            }

            corpusSize = count;

        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }
    }

    public void retrieve(String inputfilename, String outputfilename) {
        BufferedReader in = null;
        BufferedWriter out = null;

        try {
            in = new BufferedReader(new FileReader(inputfilename));
            out = new BufferedWriter(new FileWriter(outputfilename));

            String query = in.readLine();
            ArrayList<String> arrQuery;


            while (query != null) {
                long t1 = System.currentTimeMillis();

                arrQuery = new ArrayList<String>();

                if (query.trim().equals("")) {
                    query = in.readLine();
                    continue;
                }

                String delim = ",`~!@#$%^&*()-_=+{[}]|\\;\"\'<>.?/0123456789\n \t";
                StringTokenizer st = new StringTokenizer(query, delim);

                while (st.hasMoreTokens()) {
                    String s = st.nextToken();
                    arrQuery.add(s);
                }


                de = new ArrayList<DenseElement>();
                FieldType currtt = FieldType.noval;

                for (String term : arrQuery) {

                    if (!term.contains(":")) {

                        Stemmer stem = new Stemmer();

                        // case folding
                        term = term.toLowerCase();

                        // stopwords
                        if (!stopWords.contains(term)) {

                            // stemming
                            term = stem.stemIt(term);

                            de.add(findDf(term, currtt));
                        }

                    } else {
                        switch (term.charAt(0)) {
                            case 'T':
                                currtt = FieldType.title;
                                break;
                            case 'B':
                                currtt = FieldType.content;
                                break;
                            case 'I':
                                currtt = FieldType.infobox;
                                break;
                            case 'C':
                                currtt = FieldType.categories;
                                break;
                            case 'O':
                                currtt = FieldType.outlinks;
                                break;
                        }
                    }
                }

                DenseElement temp = new DenseElement();

                for (int i = 0; i < de.size(); i++) {
                    for (int j = i + 1; j < de.size(); j++) {
                        if (de.get(i) != null && de.get(j) != null) {
                            if (de.get(i).length > de.get(j).length) {
                                temp.df = de.get(i).df;
                                temp.offset = de.get(i).offset;
                                temp.tt = de.get(i).tt;
                                temp.length = de.get(i).length;

                                de.get(i).df = de.get(j).df;
                                de.get(i).offset = de.get(j).offset;
                                de.get(i).tt = de.get(j).tt;
                                de.get(i).length = de.get(j).length;

                                de.get(j).df = temp.df;
                                de.get(j).offset = temp.offset;
                                de.get(j).length = temp.length;
                            }
                        }
                    }
                }

                docListArray = new ArrayList<ArrayList<FileObject>>();

                int count = 0;

                for (int i = 0; i < de.size(); i++) {
                    if (de.get(i) != null) {
                        count++;
                    }
                }

                Thread threads[] = new Thread[de.size()];

                if (count > 1) {

                    for (int i = 0; i < de.size(); i++) {
                        docListArray.add(null);

                        if (de.get(i) != null) {
                            threads[i] = new Thread(this, Integer.toString(i));
                            threads[i].start();
                        }
                    }

                    for (int i = 0; i < threads.length; i++) {
                        if (threads[i] != null) {
                            try {
                                threads[i].join();
                            } catch (InterruptedException ex) {
                                System.out.println(ex.getMessage());
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < de.size(); i++) {
                        if (de.get(i) != null) {
                            docListArray.add(fetchDocList(de.get(i).df, de.get(i).offset));
                        } else {
                            docListArray.add(null);
                        }
                    }
                }

                int indexes[] = new int[de.size()];
                ArrayList<ArrayList<Integer>> intersection = new ArrayList<ArrayList<Integer>>();


                for (int i = 0; i < de.size(); i++) {
                    intersection.add(new ArrayList<Integer>());
                }

                if (count > 1) {

                    int j = 0;
                    while (j < docListArray.size() && docListArray.get(j) == null) {
                        j++;
                    }

                    for (int l = 0; l < docListArray.get(j).size(); l++) {
                        for (int i = j + 1; i < de.size(); i++) {
                            if (docListArray.get(i) != null) {
                                while (indexes[i] < docListArray.get(i).size() && docListArray.get(i).get(indexes[i]).docId < docListArray.get(j).get(l).docId) {
                                    indexes[i]++;
                                }
                            }
                        }

                        boolean flag = true;

                        for (int i = j + 1; i < de.size(); i++) {
                            if (docListArray.get(i) != null) {
                                if (indexes[i] < docListArray.get(i).size() && docListArray.get(i).get(indexes[i]).docId != docListArray.get(j).get(l).docId) {
                                    flag = false;
                                }
                            }
                        }

                        if (flag) {
                            intersection.get(j).add(l);

                            for (int i = j + 1; i < de.size(); i++) {
                                if (indexes[i] < docListArray.get(i).size()) {
                                    intersection.get(i).add(indexes[i]);
                                    indexes[i]++;
                                }
                            }
                        }
                    }


                    if (intersection.get(0).size() < 10) {
                        for (int l = 0; l < docListArray.size(); l++) {
                            intersection.get(l).clear();
                            for (int k = 0; k < docListArray.get(l).size(); k++) {
                                intersection.get(l).add(k);
                            }
                        }
                    }
                }

                ArrayList<Integer> finalDocListArray;

                if (count > 1) {
                    finalDocListArray = searchAndArrangeDocs(docListArray, de, intersection);
                } else {
                    finalDocListArray = searchAndArrangeDocs(docListArray, de);
                }



                //long t2 = System.currentTimeMillis();

                //out.write("Time: " + (t2 - t1) + "ms");
                //out.newLine();

                int k;
                //for (k = 0; k < MAXOUTPUT - 1 && k < finalDocListArray.size(); k++)
                for (k = 0; k <= 11 && k < finalDocListArray.size(); k++){
                    out.write(finalDocListArray.get(k) + "\t");
                    //out.write(findTitle(finalDocListArray.get(k)));
                    out.newLine();

                }

                //for (; k < MAXOUTPUT - 1; k++)
                /*
                for (; k < 10; k++){
                    out.write("NA");
                    out.newLine();

                }
                */

                //docListArray.clear();
                docListArray = null;
                arrQuery = null;
                query = in.readLine();
            }

        } catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                System.out.println("IO Error");
            }
        }
    }

    @Override
    public void run() {

        Thread t = Thread.currentThread();
        docListArray.set(Integer.parseInt(t.getName()), fetchDocList(de.get(Integer.parseInt(t.getName())).df, de.get(Integer.parseInt(t.getName())).offset));
    }

    private ArrayList<Integer> searchAndArrangeDocs(ArrayList<ArrayList<FileObject>> docListArray, ArrayList<DenseElement> de) {

        ArrayList<Integer> finalDocList = new ArrayList<Integer>();
        Double tempScore = 0d;
        ArrayList<Integer> tempDocId;

        // treemap for score and docid
        TreeMap<Double, ArrayList<Integer>> topList = new TreeMap<Double, ArrayList<Integer>>(Collections.reverseOrder());

        double idf;

        for (int i = 0; i < docListArray.size(); i++) {
            if (docListArray.get(i) != null) {
                idf = Math.log10(corpusSize / de.get(i).df);
                switch (de.get(i).tt) {
                    case title:
                        for (FileObject fo : docListArray.get(i)) {

                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {
                                tempScore = fo.title * 1.0;

                                if (fo.title > 0) {
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                    }
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength));
                                    }
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength));
                                    }
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength));
                                    }
                                }

                                tempScore = (tempScore * idf);
                            }

                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }

                        break;
                    case infobox:
                        for (FileObject fo : docListArray.get(i)) {

                            tempScore = fo.infobox * 1.0;
                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {
                                if (fo.infobox > 0) {
                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                    }
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                    }
//									if (fo.infobox > 0) {
//										tempScore += (Math.log10(fo.infobox) / Math.log10(doclength)) * WTINFOBOX;
//									}
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength));
                                    }
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength));
                                    }
                                }
                            }

                            tempScore = tempScore * idf;

                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }
                        break;
                    case outlinks:
                        for (FileObject fo : docListArray.get(i)) {

                            tempScore = fo.outlinks * 1.0;

                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {
                                if (fo.outlinks > 0) {
                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                    }
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                    }
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength));
                                    }
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength));
                                    }
//									if (fo.outlinks > 0) {
//										tempScore += (Math.log10(fo.outlinks) / Math.log10(doclength)) ;
//									}
                                }
                            }

                            tempScore = tempScore * idf;

                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }
                        break;
                    case content:
                        for (FileObject fo : docListArray.get(i)) {

                            tempScore = Math.log10(fo.content);

                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {
                                if (fo.content > 0) {
                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                    }
//									if (fo.content > 0) {
//										tempScore += (Math.log10(fo.content) / Math.log10(doclength)) ;
//									}
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength));
                                    }
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength));
                                    }
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength));
                                    }
                                }
                            }

                            tempScore = tempScore * idf;
                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }
                        break;
                    case categories:
                        for (FileObject fo : docListArray.get(i)) {

                            tempScore = fo.categories * 1.0;

                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {
                                if (fo.categories > 0) {
                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                    }
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                    }
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength));
                                    }
//									if (fo.categories > 0) {
//										tempScore += (Math.log10(fo.categories) / Math.log10(doclength));
//									}
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength));
                                    }
                                }
                            }

                            tempScore = tempScore * idf;
                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }
                        break;
                    case noval:
                        for (FileObject fo : docListArray.get(i)) {

                            tempScore = 0d;
                            int doclength = findDocLength(fo.docId);

                            if (doclength > 0) {

                                if (fo.title > 0) {
                                    tempScore += (fo.title / Math.log10(findDocLength(fo.docId))) * 5;
                                }
                                if (fo.content > 0) {
                                    tempScore += (Math.log10(fo.content) / Math.log10(doclength)) * 2;
                                }
                                if (fo.infobox > 0) {
                                    tempScore += (fo.infobox / Math.log10(doclength)) * 4;
                                }
                                if (fo.categories > 0) {

                                    tempScore += (fo.categories / Math.log10(doclength)) * 3;
                                }
                                if (fo.outlinks > 0) {
                                    tempScore += (fo.outlinks / Math.log10(doclength)) * 1;
                                }
                            }
                            tempScore = tempScore * idf;

                            tempDocId = topList.get(tempScore);

                            if (tempDocId == null) {
                                tempDocId = new ArrayList<Integer>();
                                tempDocId.add(fo.docId);
                                topList.put(tempScore, tempDocId);
                            } else {
                                tempDocId.add(fo.docId);
                            }
                        }
                        break;
                }
            }
        }


        Set<Map.Entry<Double, ArrayList<Integer>>> set1 = topList.entrySet();

        int count = 0;
        for (Map.Entry<Double, ArrayList<Integer>> e : set1) {
            count++;
            //if (count == MAXOUTPUT)
            if (count == 11){
                break;
            }
            tempDocId = e.getValue();

            for (int i : tempDocId) {
                finalDocList.add(i);
            }
        }

        return finalDocList;
    }

    private ArrayList<Integer> searchAndArrangeDocs(ArrayList<ArrayList<FileObject>> docListArray, ArrayList<DenseElement> de, ArrayList<ArrayList<Integer>> intersection) {

        ArrayList<Integer> finalDocList = new ArrayList<Integer>();
        Double tempScore;
        ArrayList<Integer> tempDocId;

        // treemap for score and docid
        TreeMap<Double, ArrayList<Integer>> topList = new TreeMap<Double, ArrayList<Integer>>(Collections.reverseOrder());
        TreeMap<Integer, Double> topDocid = new TreeMap<Integer, Double>();

        double idf;
        for (int i = 0; i < docListArray.size(); i++) {
            if (docListArray.get(i) != null) {
                idf = Math.log10(corpusSize / de.get(i).df);
                switch (de.get(i).tt) {
                    case title:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    tempScore = fo.title * 1.0;

                                    if (fo.title > 0) {
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }

                                    tempScore = (tempScore * idf);
                                }

                                topDocid.put(fo.docId, tempScore);
                            } else {

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    tempScore += fo.title;

                                    if (fo.title > 0) {
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }

                                    tempScore = (tempScore * idf);
                                }

                                topDocid.put(fo.docId, tempScore);
                            }
                        }

                        break;
                    case infobox:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {
                                tempScore = fo.infobox * 1.0;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.infobox > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += ((fo.content) / Math.log10(doclength));
                                        }
//									if (fo.infobox > 0) {
//										tempScore += (Math.log10(fo.infobox) / Math.log10(doclength)) * WTINFOBOX;
//									}
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;

                                topDocid.put(fo.docId, tempScore);
                            } else {
                                tempScore += fo.infobox;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.infobox > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
//									if (fo.infobox > 0) {
//										tempScore += (Math.log10(fo.infobox) / Math.log10(doclength)) * WTINFOBOX;
//									}
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            }
                        }
                        break;
                    case outlinks:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {
                                tempScore = fo.outlinks * 1.0;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.outlinks > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
//									if (fo.outlinks > 0) {
//										tempScore += (Math.log10(fo.outlinks) / Math.log10(doclength)) * WTOUTLINKS;
//									}
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            } else {
                                tempScore += fo.outlinks;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.outlinks > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
//									if (fo.outlinks > 0) {
//										tempScore += (Math.log10(fo.outlinks) / Math.log10(doclength)) * WTOUTLINKS;
//									}
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            }
                        }
                        break;
                    case content:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {

                                int doclength = findDocLength(fo.docId);

                                tempScore = Math.log10(fo.content) / doclength;

                                if (doclength > 0) {
                                    if (fo.content > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
//									if (fo.content > 0) {
//										tempScore += (Math.log10(fo.content) / Math.log10(doclength)) * WTCONTENT;
//									}
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            } else {

                                int doclength = findDocLength(fo.docId);

                                tempScore += Math.log10(fo.content) / doclength;

                                if (doclength > 0) {
                                    if (fo.content > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
//									if (fo.content > 0) {
//										tempScore += (Math.log10(fo.content) / Math.log10(doclength)) * WTCONTENT;
//									}
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
                                        if (fo.categories > 0) {

                                            tempScore += (fo.categories / Math.log10(doclength));
                                        }
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            }
                        }
                        break;
                    case categories:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {
                                tempScore = fo.categories * 1.0;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.categories > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
//									if (fo.categories > 0) {
//										tempScore += (Math.log10(fo.categories) / Math.log10(doclength)) * WTCATEGORIES;
//									}
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            } else {
                                tempScore += fo.categories * 1.0;

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {
                                    if (fo.categories > 0) {
                                        if (fo.title > 0) {
                                            tempScore += (fo.title / Math.log10(findDocLength(fo.docId)));
                                        }
                                        if (fo.content > 0) {
                                            tempScore += (Math.log10(fo.content) / Math.log10(doclength));
                                        }
                                        if (fo.infobox > 0) {
                                            tempScore += (fo.infobox / Math.log10(doclength));
                                        }
//									if (fo.categories > 0) {
//										tempScore += (Math.log10(fo.categories) / Math.log10(doclength)) * WTCATEGORIES;
//									}
                                        if (fo.outlinks > 0) {
                                            tempScore += (fo.outlinks / Math.log10(doclength));
                                        }
                                    }
                                }

                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            }
                        }
                        break;
                    case noval:
                        for (int j : intersection.get(i)) {
                            FileObject fo = docListArray.get(i).get(j);

                            tempScore = topDocid.get(fo.docId);

                            if (tempScore == null) {
                                tempScore = 0d;
                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {

                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId))) * 5;
                                    }
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength)) * 2;
                                    }
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength)) * 4;
                                    }
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength)) * 3;
                                    }
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength)) * 1;
                                    }

                                }
                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            } else {

                                int doclength = findDocLength(fo.docId);

                                if (doclength > 0) {

                                    if (fo.title > 0) {
                                        tempScore += (fo.title / Math.log10(findDocLength(fo.docId))) * 5;
                                    }
                                    if (fo.content > 0) {
                                        tempScore += (Math.log10(fo.content) / Math.log10(doclength)) * 2;
                                    }
                                    if (fo.infobox > 0) {
                                        tempScore += (fo.infobox / Math.log10(doclength)) * 4;
                                    }
                                    if (fo.categories > 0) {

                                        tempScore += (fo.categories / Math.log10(doclength)) * 3;
                                    }
                                    if (fo.outlinks > 0) {
                                        tempScore += (fo.outlinks / Math.log10(doclength)) * 1;
                                    }

                                }
                                tempScore = tempScore * idf;
                                topDocid.put(fo.docId, tempScore);
                            }
                        }
                        break;
                }
            }
        }

        Set<Map.Entry<Integer, Double>> set = topDocid.entrySet();

        for (Map.Entry<Integer, Double> e : set) {

            if (e.getValue() == null) {
                continue;
            }
            tempDocId = null;

            try {
                tempDocId = topList.get(e.getValue());
            } catch (Exception ex) {
                System.out.println(e);
                System.exit(1);
            }

            if (tempDocId == null) {
                tempDocId = new ArrayList<Integer>();
                tempDocId.add(e.getKey());
                topList.put(e.getValue(), tempDocId);
            } else {
                tempDocId.add(e.getKey());
            }
        }

        Set<Map.Entry<Double, ArrayList<Integer>>> set1 = topList.entrySet();

        int count = 0;
        for (Map.Entry<Double, ArrayList<Integer>> e : set1) {
            count++;
            //if (count == MAXOUTPUT) 
            if (count == 11){
                break;
            }
            tempDocId = e.getValue();

            for (int i : tempDocId) {
                finalDocList.add(i);
            }
        }

        return finalDocList;
    }

    private ArrayList<FileObject> fetchDocList(int df, long offsetMain) {

        BufferedReader mainIn = null;
        RandomAccessFile mainFile = null;
        ArrayList<FileObject> docs = null;

        try {
            // move the file to offset				
            mainFile = new RandomAccessFile(outputdirname + "finalIndex/Output_Main", "r");
            mainFile.seek(offsetMain);
            mainIn = new BufferedReader(new FileReader(mainFile.getFD()));

            docs = new ArrayList<FileObject>(df);

            FileObject fo = null;

            StringBuilder temp = new StringBuilder(mainIn.readLine());

            int startIndex = 0;
            int endIndex = temp.indexOf("#");
            String currDoc = temp.substring(startIndex, endIndex);

            int elementStartIndex = 0;
            int elementEndIndex = 0;

            int previousDocId = 0;

            while (endIndex != -1) {
                fo = new FileObject();
                fo.makezero();

                elementEndIndex = currDoc.indexOf(":");

                fo.docId = Integer.parseInt(currDoc.substring(elementStartIndex, elementEndIndex), 16);
                fo.docId += previousDocId;
                previousDocId = fo.docId;

                elementStartIndex = elementEndIndex + 1;
                elementEndIndex = currDoc.indexOf(":", elementStartIndex);

                while (elementEndIndex != -1) {
                    if (elementEndIndex - elementStartIndex < 5) {
                        switch (currDoc.charAt(elementStartIndex)) {
                            case 'T':
                                fo.title = (byte) Integer.parseInt(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16);
                                break;
                            case 'C':
                                fo.content = (byte) Integer.parseInt(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16);
                                break;
                            case 'I':
                                fo.infobox = (byte) Integer.parseInt(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16);
                                break;
                            case 'O':
                                fo.outlinks = (byte) Integer.parseInt(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16);
                                break;
                            case 'G':
                                fo.categories = (byte) Integer.parseInt(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16);
                                break;
                        }
                    } else {
                        switch (currDoc.charAt(elementStartIndex)) {
                            case 'T':
                                fo.title = (int) (Long.parseLong("ffffffff", 16) - Long.parseLong(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16)) + 127;
                                break;
                            case 'C':
                                fo.content = (int) (Long.parseLong("ffffffff", 16) - Long.parseLong(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16)) + 127;
                                break;
                            case 'I':
                                fo.infobox = (int) (Long.parseLong("ffffffff", 16) - Long.parseLong(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16)) + 127;
                                break;
                            case 'O':
                                fo.outlinks = (int) (Long.parseLong("ffffffff", 16) - Long.parseLong(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16)) + 127;
                                break;
                            case 'G':
                                fo.categories = (int) (Long.parseLong("ffffffff", 16) - Long.parseLong(currDoc.substring(elementStartIndex + 1, elementEndIndex), 16)) + 127;
                                break;
                        }
                    }



                    elementStartIndex = elementEndIndex + 1;
                    elementEndIndex = currDoc.indexOf(":", elementStartIndex);
                }

                startIndex = endIndex;
                endIndex = temp.indexOf("#", startIndex + 1);
                if (endIndex != -1) {
                    currDoc = temp.substring(startIndex + 1, endIndex);
                }

                elementStartIndex = 0;
                elementEndIndex = 0;

                docs.add(fo);
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                mainIn.close();
                mainFile.close();
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return docs;
    }

    private int findDocLength(int docid) {

        int retVal = docLength.get(docid);

        return retVal;
    }

    private String findTitle(int docid) {
        BufferedReader forwardIn = null;
        RandomAccessFile forwardFile = null;

        String retVal = "";


        try {
            Integer floor;

            floor = forwardIndex.floorKey(docid);
            int offsetFloor = 0;
            if (floor != null) {
                offsetFloor = forwardIndex.get(floor);
            }

            forwardFile = new RandomAccessFile(outputdirname + "finalIndex/Output_Forward", "r");
            forwardFile.seek(offsetFloor);
            forwardIn = new BufferedReader(new FileReader(forwardFile.getFD()));

            String s = "";
            boolean isFound = false;

            for (int i = 0; i < SPARSEVAL && s != null; i++) {
                s = forwardIn.readLine();

                if (s != null && s.startsWith(Integer.toHexString(docid) + ":")) {
                    isFound = true;
                    break;
                }
            }

            if (isFound) {
                if (s != null) {

                    String as[] = s.split(":");

                    for (int i = 2; i < as.length; i++) {
                        retVal += as[i];
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                forwardIn.close();
                forwardFile.close();

            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return retVal;
    }

    private DenseElement findDf(String term, FieldType currtt) {
        BufferedReader secondIn = null;
        RandomAccessFile secondFile = null;

        DenseElement de1 = new DenseElement();

        try {
            String floor;
            String ceiling;

            floor = tertiaryIndex.floorKey(term);
            int offsetFloor = 0;
            if (floor != null) {
                offsetFloor = tertiaryIndex.get(floor);
            }

            ceiling = tertiaryIndex.ceilingKey(term);
            int offsetCeiling = 0;
            if (ceiling != null) {
                offsetCeiling = tertiaryIndex.get(ceiling);
            }

            secondFile = new RandomAccessFile(outputdirname + "finalIndex/Output_Second", "r");
            secondFile.seek(offsetFloor);
            secondIn = new BufferedReader(new FileReader(secondFile.getFD()));

            String s = "";
            boolean isFound = false;

            for (int i = 0; i < SPARSEVAL && s != null; i++) {
                s = secondIn.readLine();

                if (s.startsWith(term + ":")) {
                    isFound = true;
                    break;
                }
            }

            if (isFound) {
                if (s != null) {

                    String as[] = s.split(":");

                    de1.offset = Long.parseLong(as[1], 16);
                    de1.df = Integer.parseInt(as[2], 16);
                    de1.tt = currtt;
                    de1.length = offsetCeiling - offsetFloor;
                }
            } else {
                de1 = null;
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (secondIn != null) {
                    secondIn.close();
                }
                if (secondFile != null) {
                    secondFile.close();
                }

            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return de1;
    }

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
}
