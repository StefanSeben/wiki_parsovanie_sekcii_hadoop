package WikiDumpParser;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.surround.query.SimpleTerm;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiTextSearch
{
    private static String format;

    private static final String[] VALID_FORMAT_NAMES = {"redirsFrom", "pageTitle","sectionTitle","sectionText","redirsCount"};
    private static final String DEFAULT_SEARCH_FIELD = "sectionText";
    private static final String DELIMETER = "█";

    public static void main( String[] args ) throws IOException, ParseException {
        StopAnalyzer analyzer = new StopAnalyzer();

        Path indexPath = Paths.get(args[0]);
        Directory index = new SimpleFSDirectory(indexPath);
       //Directory index = new RAMDirectory();

        if (!Files.exists(indexPath) || args.length > 1) {
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            IndexWriter w = new IndexWriter(index, config);

            for (int i = 1; i < args.length; ++i) {
                File f = new File(args[i]);
                BufferedReader reader = new BufferedReader(new FileReader(f));

                String pageTitle = "";
                String sectionTitle = "";
                String sectionText = "";
                int redirCount = 0;
                String[] redirsFrom;

                int c = 0;
                String line = reader.readLine();
                while (line != null) {
                    String[] valueSplits = line.split("███");

                    try {
                        if (valueSplits.length > 4) {
                            pageTitle = valueSplits[0].split("\t")[1];
                            sectionTitle = valueSplits[1];
                            redirCount = Integer.parseInt(valueSplits[2]);

                            redirsFrom = new String[redirCount];
                            for (int j = 0; j < redirCount; ++j) {
                                redirsFrom[j] = valueSplits[3 + j];
                            }

                            if (3 + redirCount == valueSplits.length - 1) {
                                sectionText = valueSplits[3 + redirCount];

                                addDoc(w, pageTitle, sectionTitle, sectionText, redirsFrom);
                            }
                        }
                    } catch (IndexOutOfBoundsException e) {
                        System.err.println(line);
                    }

                    line = reader.readLine();
                }


                reader.close();
                w.commit();
            }

            w.close();
        }

        int hitsPerPage = 10;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        String querystr = "not empty";

        Scanner keyboard = new Scanner(System.in);
        System.out.println("enter query to search: ");
        querystr = keyboard.nextLine();

        List<String> display = new ArrayList<>();
        display.add("redirsFrom");
        display.add("pageTitle");
        display.add("sectionTitle");

        setFormatText(display);

        int i = 0;
        ScoreDoc[] hits = {};
        Query q = null;
        String luceneQuery = "";
        while (!querystr.toLowerCase().replaceAll("\\s", "").equals("quit")) {
            if (!querystr.equals("") || !luceneQuery.equals("")) {
                if (querystr.equals("")) {
                    TopDocs docs = searcher.search(q, i + hitsPerPage);
                    hits = docs.scoreDocs;

                    System.out.println("Found " + (hits.length - i) + " hits for: " + luceneQuery);
                }
                else {
                    luceneQuery = parseQuery(querystr, display);

                    if (!luceneQuery.isEmpty()) {
                        q = new QueryParser(DEFAULT_SEARCH_FIELD, analyzer).parse(luceneQuery);

                        TopDocs docs = searcher.search(q, hitsPerPage);
                        hits = docs.scoreDocs;

                        System.out.println("Found " + hits.length + " hits for: " + luceneQuery);
                        i = 0;
                    }
                }

                if (i > 0 && i == hits.length) {
                    System.out.println("No more results.");
                }
                else if (!display.isEmpty()) {
                    System.out.println(format);

                    for (; i < hits.length; ++i) {
                        int docId = hits[i].doc;
                        Document d = searcher.doc(docId);

                        String toPrint = (i + 1) + ". ";

                        for (String field : display)
                            toPrint += d.get(field) + DELIMETER;

                        System.out.println(toPrint);
                    }
                }
            }


            System.out.println("enter query to search: ");
            querystr = keyboard.nextLine();
        }

        reader.close();
    }

    private static void setFormatText(List<String> toDisplay) {
        format = "FORMAT: ";
        for (String field : toDisplay)
            format += field + DELIMETER;
    }

    private static void setDefaultFormat(List<String> toDisplay) {
        toDisplay.clear();
        toDisplay.add("redirsFrom");
        toDisplay.add("pageTitle");
        toDisplay.add("sectionTitle");

        setFormatText(toDisplay);
    }

    private static void addAllFormatFields(List<String> toDisplay) {
        toDisplay.clear();
        for (int i = 0; i < VALID_FORMAT_NAMES.length; ++i) {
            toDisplay.add(VALID_FORMAT_NAMES[i]);
        }
    }

    private static boolean isValidFormatName(String s) {
        for (int i = 0; i < VALID_FORMAT_NAMES.length; ++i) {
            if (s.equals(VALID_FORMAT_NAMES[i]))
                return true;
        }

        System.err.println(s + " is not a valid field name.");

        return false;
    }

    private static String parseQuery(String query, List<String> display) {
        Matcher queryMatcher = Pattern.compile("\\s+[^\\+\\- ]").matcher(query);

        String luceneQuery = "";
        int luceneQueryStart = query.length();
        if (queryMatcher.find()) {
            luceneQueryStart = queryMatcher.start();

            luceneQuery = query.substring(luceneQueryStart);
        }

        queryMatcher = Pattern.compile("([\\+\\-].*?)\\s").matcher(query.substring(0, luceneQueryStart) + " ");

        boolean found = false;
        while (queryMatcher.find()) {
            found = true;

            char startChar = queryMatcher.group(1).charAt(0);
            String querySplit = queryMatcher.group(1).substring(1);

            if (startChar == '-') {
                if (querySplit.equals("-"))
                    display.clear();
                else if (querySplit.equals("+"))
                    setDefaultFormat(display);
                else if (isValidFormatName(querySplit))
                    display.remove(querySplit);
            } else if (startChar == '+') {
                if (querySplit.equals("+"))
                    addAllFormatFields(display);
                else if (querySplit.equals("-"))
                    setDefaultFormat(display);
                else if (isValidFormatName(querySplit) && !display.contains(querySplit))
                    display.add(querySplit);
            } else {
                System.err.println("Invalid query: " + queryMatcher.group(1));
            }
        }

        if (!found)
            luceneQuery = query;
        else {
            setFormatText(display);
        }

        return luceneQuery;
    }

    private static void addDoc(IndexWriter w, String pageTitle, String sectionTitle, String sectionText, String[] redirsFrom) throws IOException {
        Document doc = new Document();

        doc.add(new TextField("pageTitle", pageTitle, Field.Store.YES));
        doc.add(new TextField("sectionTitle", sectionTitle, Field.Store.YES));
        doc.add(new TextField("sectionText", sectionText, Field.Store.YES));
        doc.add(new IntField("redirsCount", redirsFrom.length, Field.Store.YES));

        String redirs = "";
        for (int i = 0; i < redirsFrom.length; ++i)
            redirs += redirsFrom[i] + "\t";

        doc.add(new TextField("redirsFrom", redirs, Field.Store.YES));

        w.addDocument(doc);
    }

}
