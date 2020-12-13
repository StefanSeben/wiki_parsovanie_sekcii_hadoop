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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiTextSearch
{
    private static String format;

    private static final String[] VALID_FORMAT_NAMES = {"redirsFrom", "pageTitle","sectionTitle","sectionText","redirsCount"};
    private static final String DEFAULT_SEARCH_FIELD = "sectionText";
    private static final String DELIMETER = "█";
    private static final int HITS_PER_PAGE = 10;

    private static final int AVG_SENTENCE_LENGTH = 85;
    private static final int TEST_SENTENCE_LENGTH = 10;


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

                WikiSection longestText = new WikiSection();
                WikiSection mostRedirects = new WikiSection();

                float avgTextLength = 0;
                float avgRedirects = 0;
                int redirCount = 0;
                int lessThanSentence = 0;
                int lessThanTest = 0;
                int one = 0;
                int zero = 0;
                Map<Integer, Integer> redirsCounts = new HashMap<>();
                int c = 0;

                System.out.println("Creating index for: " + args[i]);

                String line = reader.readLine();
                while (line != null) {
                    String[] valueSplits = line.split("███");
                    WikiSection section = new WikiSection();

                    try {
                        if (valueSplits.length > 4) {
                            section.setPageTitle(valueSplits[0].split("\t")[1]);
                            section.setSectionTitle(valueSplits[1]);
                            redirCount = Integer.parseInt(valueSplits[2]);

                            for (int j = 0; j < redirCount; ++j) {
                                section.addRedirFrom(valueSplits[3 + j]);
                            }

                            if (3 + redirCount == valueSplits.length - 1) {
                                section.setSectionText(valueSplits[3 + redirCount]);

                                addDoc(w, section);

                                if (section.getSectionText().length() > longestText.getSectionText().length())
                                    longestText = section;
                                if (redirCount > mostRedirects.getRedirCount())
                                    mostRedirects = section;
                                avgRedirects += redirCount;
                                avgTextLength += section.getSectionText().length();
                                if (section.getSectionText().length() < AVG_SENTENCE_LENGTH)
                                    ++lessThanSentence;
                                if (section.getSectionText().length() < TEST_SENTENCE_LENGTH)
                                    ++lessThanTest;
                                if (section.getSectionText().length() == 1)
                                    ++one;
                                if (section.getSectionText().length() == 0)
                                    ++zero;
                                if (!redirsCounts.containsKey(redirCount))
                                    redirsCounts.put(redirCount, 1);
                                else
                                    redirsCounts.put(redirCount, redirsCounts.get(redirCount) + 1);

                                ++c;
                            }
                        }
                    } catch (IndexOutOfBoundsException e) {
                        System.err.println(line);
                    }

                    line = reader.readLine();
                }

                reader.close();
                w.commit();

                avgRedirects /= c;
                avgTextLength /= c;

                System.out.println("Successfully finished creating index for: " + args[i]);
                System.out.println("Stats: ");
                System.out.println("Longest section: " + longestText.getSectionText().length()
                        + " \"" + longestText.getSectionTitle() + "\" (page: " + longestText.getPageTitle() + ")");
                System.out.println("Most redirects: " + mostRedirects.getRedirCount()
                        + " \"" + mostRedirects.getSectionTitle() + "\" (page: " + mostRedirects.getPageTitle() + ")");
                System.out.println("Avg. section length: " + avgTextLength);
                System.out.println("Avg. redir count: " + avgRedirects);
                System.out.println("Sections shorter than avg. sentence length: " + lessThanSentence);
                System.out.println("Sections shorter than " + TEST_SENTENCE_LENGTH + " characters: " + lessThanTest);
                System.out.println("Sections with 1 character: " + one);
                System.out.println("Sections with 0 characters: " + zero);

                FileWriter redirsCountWriter = new FileWriter(args[i] + "redirsCount.txt");
                for ( Map.Entry<Integer, Integer> redirCountEntry : redirsCounts.entrySet()) {
                    redirsCountWriter.write(redirCountEntry.getKey() + "\t" + redirCountEntry.getValue() + "\n");
                }
                redirsCountWriter.close();
            }

            w.close();
        }

        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        String querystr = "not empty";

        Scanner keyboard = new Scanner(System.in);
        System.out.print("enter query to search: ");
        querystr = keyboard.nextLine();

        List<String> display = new ArrayList<>();

        addAllFormatFields(display);
        setFormatText(display);

        int i = 0;
        ScoreDoc[] hits = {};
        Query q = null;
        String luceneQuery = "";
        while (!querystr.toLowerCase().replaceAll("\\s", "").equals("quit")) {
            if (!querystr.equals("") || !luceneQuery.equals("")) {
                if (querystr.equals("")) {
                    TopDocs docs = searcher.search(q, i + HITS_PER_PAGE);
                    hits = docs.scoreDocs;

                    System.out.println("Found " + (hits.length - i) + " hits for: " + luceneQuery);
                }
                else {
                    luceneQuery = parseQuery(querystr, display);

                    if (!luceneQuery.isEmpty()) {
                        q = new QueryParser(DEFAULT_SEARCH_FIELD, analyzer).parse(luceneQuery);

                        TopDocs docs = searcher.search(q, HITS_PER_PAGE);
                        hits = docs.scoreDocs;

                        System.out.println("Found " + hits.length + " hits for: " + luceneQuery);
                        i = 0;
                    }
                }

                if (i > 0 && i == hits.length) {
                    System.out.println("No more results.");
                }
                else if ( hits.length != 0 && !display.isEmpty()) {
                    System.out.println(format);

                    for (; i < hits.length; ++i) {
                        int docId = hits[i].doc;
                        Document d = searcher.doc(docId);

                        String toPrint = (i + 1) + ". ";

                        for (String field : display)
                            toPrint += d.get(field) + DELIMETER;

                        System.out.println(toPrint);
                    }

                    if (i % HITS_PER_PAGE == 0)
                        System.out.println("enter empty query to display more results...");
                    else
                        System.out.println("No more results.");
                }
            }


            System.out.println();
            System.out.print("enter query to search: ");
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

    private static void addDoc(IndexWriter w, WikiSection section) throws IOException {
        Document doc = new Document();

        doc.add(new TextField("pageTitle", section.getPageTitle(), Field.Store.YES));
        doc.add(new TextField("sectionTitle", section.getSectionTitle(), Field.Store.YES));
        doc.add(new TextField("sectionText", section.getSectionText(), Field.Store.YES));
        doc.add(new IntField("charCount", section.getSectionText().length(), Field.Store.YES));
        doc.add(new IntField("redirsCount", section.getRedirCount(), Field.Store.YES));

        String redirs = "";
        for (String redir : section.getRedirsFrom())
            redirs += redir + "\t";

        doc.add(new TextField("redirsFrom", redirs, Field.Store.YES));

        w.addDocument(doc);
    }

}
