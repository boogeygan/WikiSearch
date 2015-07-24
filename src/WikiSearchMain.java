package dom.gun.ire.minor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * @author gagan
 */
public class WikiSearchMain {

    public static void main(String args[]) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String indexOutputDIR = "/home/gagan/Downloads/Compressed/OutputIndex/";
            String inputfilepath = "resources/Input.txt";
            String outputfilepath = "resources/Output.txt";
            
            //System.exit(1);
            /*
            WikiPageIndexer index = null;//new Indexer(indexOutputDIR);
            WikiXMLParser xmlParser = new WikiXMLParser(indexOutputDIR);

            //give the XML Corpus absolute path here to XML xmlParser...

            index = xmlParser.readXMLFile("/home/gagan/Downloads/Compressed/enwiki-latest-pages-articles.xml");
            xmlParser.out.close();
            index.writeToFile(indexOutputDIR + "temp/output_last");
            index.MergeFiles();
            index.createTertiary();
            */

            System.out.println("Fetching Index...");
            Retriever retriever = new Retriever(indexOutputDIR);
            System.out.println("Searcing...");
            retriever.retrieve(inputfilepath, outputfilepath);
            System.out.println("Completed...!");
        } catch (Exception ex) {
            System.err.println(ex);
        }
    }
}
