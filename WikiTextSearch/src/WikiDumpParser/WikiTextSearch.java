package WikiDumpParser;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class WikiTextSearch
{
    public static void main( String[] args ) throws IOException, ParseException {

        SimpleAnalyzer analyzer = new SimpleAnalyzer();

        Directory index = new SimpleFSDirectory(Paths.get(args[0]));

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, config);

        for (int i = 1; i < args.length; ++i) {
            File f = new File(args[i]);
            BufferedReader reader = new BufferedReader(new FileReader(f));

            String title = "";
            String text = "";

            String line = reader.readLine();
            while (line != null) {
                Matcher m = Pattern.compile("[^\\s<>\\[#\\]]*#[^\\s<>\\[#\\]]*")
                        .matcher(line);
                if (m.find()) {
                    if (title != "")
                        addDoc(w, title, text);
                    title = m.group();
                    text = line.split("[^\\s<>\\[#\\]]*#[^\\s<>\\[#\\]]*\\t")[1];
                }
                else
                    text += line;

                line = reader.readLine();
            }
            if (title != "")
                addDoc(w, title, text);

            reader.close();
            w.commit();
        }

        w.close();

        int hitsPerPage = 10;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        String querystr = "not empty";

        Scanner keyboard = new Scanner(System.in);
        System.out.println("enter text to search: ");
        querystr = keyboard.nextLine();
        while (!querystr.equals("")) {
            Query q = new QueryParser("sectionText", analyzer).parse(querystr);

            TopDocs docs = searcher.search(q, hitsPerPage);
            ScoreDoc[] hits = docs.scoreDocs;

            System.out.println("Found " + hits.length + " hits.");
            for (int i = 0; i < hits.length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                System.out.println((i + 1) + ". " + d.get("sectionName") + "\t" + d.get("sectionText"));
            }

            System.out.println("enter text to search: ");
            querystr = keyboard.nextLine();
        }

        reader.close();
    }

    private static void addDoc(IndexWriter w, String sectionName, String sectionText) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("sectionName", sectionName, Field.Store.YES));

        doc.add(new TextField("sectionText", sectionText, Field.Store.YES));
        w.addDocument(doc);
    }

}
