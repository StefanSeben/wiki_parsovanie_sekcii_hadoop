import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MY class. Parses Wiki XML dump. Extracts texts of sections, which are redirected from a redirect page.
 * To be used with Hadoop 3.x.x.
 */
public class WikiSectionParser {

    private static final String REDIRECTED = "-r";

    /**
     * Mapper class for @WikiSectionParser. Executes the mapping step.
     */
    public static class WikiSectionMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private static List<WikiPage> parsedPages = new ArrayList<>();

        /**
         * Extracts sections from wiki page.
         * @param page page to parse
         * @param sections list of parsed sections parsed from @page
         * @throws UnsupportedEncodingException error while decoding UTF-8 characters
         */
        public static void parseSections(WikiPage page, List<WikiPage> sections) throws UnsupportedEncodingException {
            Stack<String> regex = new Stack();

            regex.push("(?<!=)======\\s*([^=]+)\\s*======(?!=)");
            regex.push("(?<!=)=====\\s*([^=]+)\\s*=====(?!=)");
            regex.push("(?<!=)====\\s*([^=]+)\\s*====(?!=)");
            regex.push("(?<!=)===\\s*([^=]+)\\s*===(?!=)");
            regex.push("(?<!=)==\\s*([^=]+)\\s*==(?!=)");

            parseRecursively(page, sections, regex);
        }

        /**
         * Recursively extracts sections from wiki page until regex stack is emptied.
         * @param page page to parse
         * @param sections list of parsed sections parsed from @page
         * @param regex stack of regular expression used to parse @page
         * @throws UnsupportedEncodingException error while decoding UTF-8 characters
         */
        private static void parseRecursively(WikiPage page, List<WikiPage> sections, Stack<String> regex) throws UnsupportedEncodingException {
            Matcher secMatcher = Pattern.compile(regex.pop())
                    .matcher(page.getPageText());

            WikiPage newSection = null;
            String sectionTitle = null;
            int s = 0,e = -1;
            while (secMatcher.find()) {
                if (s > 0) {
                    e = secMatcher.start();

                    newSection = new WikiPage();
                    newSection.setPageTitle(sectionTitle);
                    newSection.setPageText(page.getPageText().substring(s, e));

                    if (!regex.isEmpty()) {
                        parseRecursively(new WikiPage(page.getPageTitle(), newSection.getPageText())
                                , sections
                                , (Stack<String>) regex.clone());
                    }

                    sections.add(newSection);
                }

                sectionTitle = (page.getPageTitle()
                        + "#"
                        + secMatcher.group(0)
                        .replaceAll("=","")
                        .replaceAll("^\\s+","")
                        .replaceAll("\\s+$","")
                );
                sectionTitle = decodeUndecodedUTF(stripTitle(sectionTitle));

                s = secMatcher.end();
            }

            if (s > 0) {
                e = page.getPageText().length();

                newSection = new WikiPage();
                newSection.setPageTitle(sectionTitle);
                newSection.setPageText(page.getPageText().substring(s, e));

                if (!regex.isEmpty()) {
                    parseRecursively(new WikiPage(page.getPageTitle(), newSection.getPageText())
                            , sections
                            , (Stack<String>) regex.clone());
                }

                sections.add(newSection);
            }
        }

        /**
         * Parses redirect page and extracts the redirect.
         * @param page page to parse
         * @return parsed redirect. returns "" if parsed page is not redirect page to section
         */
        public static String parseRedirectPages(WikiPage page) {
            String redirectedPage = "";

            Matcher m = Pattern.compile("(?i)^(\\s*#redirect\\s*\\[\\[(.+?#.+?)\\]\\])"
                    + "|(\\s*#presmeruj\\s*\\[\\[(.+?#.+?)\\]\\])")
                    .matcher(page.getPageText());
            if (m.find()) {
                redirectedPage = (m.group(2) == null ? m.group(4) : m.group(2));
            }

            return redirectedPage;
        }

        /**
         * Decodes UTF-8 characters in given text.
         * @param text text to decode
         * @return text with decoded UTF-8 characters
         * @throws UnsupportedEncodingException error while decoding UTF-8 characters
         */
        public static String decodeUndecodedUTF(String text) throws UnsupportedEncodingException {
            Matcher utfMatcher = Pattern.compile("(?i)((%[0-9a-e][0-9a-e])+)|((.[0-9a-e][0-9a-e])+)").matcher(text);

            String decoded = "";

            int s = 0,e = 0;
            while (utfMatcher.find()) {
                s = utfMatcher.start();

                decoded += text.substring(e, s);

                e = utfMatcher.end();

                String utfString = utfMatcher.group(3) == null
                        ? utfMatcher.group(1).replace(".", "%")
                        : utfMatcher.group(3).replace(".", "%");

                decoded += URLDecoder.decode(utfString, "UTF-8");
            }

            decoded += text.substring(e);

            return decoded;
        }

        /**
         * Extracts Wiki anchors from text.
         * @param text text to parse
         * @return list of anchors in the text
         */
        public static String[] extractAnchors(String text) {
            Matcher anchorMatcher = Pattern.compile("(?i)\\{\\{\\s*anchor\\s*\\|(.*?)\\}\\}"
                    + "|\\{\\{\\s*kotva\\s*\\|(.*?)\\}\\}").matcher(text);

            String[] res = null;
            if (anchorMatcher.find()) {
                String anchor = anchorMatcher.group(1) == null ? anchorMatcher.group(2) : anchorMatcher.group(1);

                String[] textSplits = text.split("#");
                String[] anchorSplits = anchor.split("\\|");
                res = new String[anchorSplits.length];

                for (int i = 0; i < anchorSplits.length; ++i) {
                    if (textSplits.length > 1)
                        res[i] = text.split("#")[0] + "#" + anchorSplits[i];
                    else
                        res[i] = anchorSplits[i];
                }
            }
            else {
                res = new String[1];
                res[0] = text;
            }

            return res;
        }

        /**
         * Repalces references with names of references.
         * @param text text to parse
         * @return text with references replaced with names of references
         */
        public static String extractReferences(String text) {
            Matcher referenceMatcher = Pattern.compile(//"(?i)\\[\\[:Category:(.*?)\\]\\]"
                    //+ "|\\[\\[:File:(.*?)\\]\\]|" +
                    "\\[\\[([^#]+?)\\]\\]").matcher(text);

            String extracted = "";
            int s = 0, e = 0;
            while (referenceMatcher.find()) {
                s = referenceMatcher.start();

                extracted += text.substring(e, s);

                e = referenceMatcher.end();

                String reference = referenceMatcher.group(1) /*== null
                        ?  referenceMatcher.group(2) == null ? referenceMatcher.group(3) : referenceMatcher.group(2)
                        : referenceMatcher.group(1)*/;
                String[] referenceSplits = reference.split("\\|");

                if (referenceSplits.length <= 1)
                    extracted += reference
                            .replaceAll("^\\s+", "")
                            .replaceAll("\\s+$", "");
                else
                    extracted += referenceSplits[1]
                            .replaceAll("^\\s+", "")
                            .replaceAll("\\s+$", "");
            }

            extracted += text.substring(e);

            return extracted;
        }

        /**
         * Removes Wiki markup from section title.
         * @param title title to parse
         * @return title with removed Wiki markup
         */
        public static String stripTitle(String title) {
            return extractReferences(title
                    //.replaceAll("(?i)\\[\\[File:.*?\\]\\]", "")
                    //.replaceAll("(?i)\\[\\[Category:.*?\\]\\]", "")
                    .replaceAll("(?i)<ref.*?>.*?</ref>", "")
                    .replaceAll("\\{\\{cn\\}\\}", "")
                    .replaceAll("(?i)<s>|</s>", "")
                    .replaceAll("(?i)<u>|</u>", "")
                    .replaceAll("<!--.*?-->", "")
                    //.replaceAll("~~~~", "")
                    .replaceAll("-", "–")
                    .replaceAll(" ", " ")
                    .replaceAll("_", " ")
                    .replaceAll("''", "")
                    .replaceAll("'''", "")
                    .replaceAll("^\\s+","")
                    .replaceAll("\\s+$","")
                    .replaceAll("\\p{Cc}", "")
            );
        }

        /**
         * Map step. Extracts sections and redirect pages to sections from XML Wiki dump.
         * @param key id of page
         * @param text xml wiki page
         * @param context hadoop execution context
         * @throws IOException
         */
        public void map(LongWritable key, Text text, Context context
        ) throws IOException {
            try {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(new InputSource(new StringReader(text.toString())));

                NodeList titles = doc.getElementsByTagName("title");
                NodeList redirects = doc.getElementsByTagName("redirect");
                NodeList texts = doc.getElementsByTagName("text");
                NodeList models = doc.getElementsByTagName("model");

                if (models.getLength() > 0 && models.item(0).getTextContent().equals("wikitext")) {
                    WikiPage parsedPage = new WikiPage();

                    parsedPage.setPageTitle(decodeUndecodedUTF(stripTitle(titles.item(0).getTextContent())));
                    parsedPage.setPageText(texts.item(0).getTextContent().replaceAll("\\n", " "));

                    if (redirects.getLength() > 0) {
                        String redirectedPage = decodeUndecodedUTF(stripTitle(parseRedirectPages(parsedPage)));

                        if (!redirectedPage.isEmpty()) {
                            context.write(new Text(redirectedPage.toLowerCase())
                                    , new Text(REDIRECTED + parsedPage.getPageTitle()));
                        }
                    }
                    else {
                        List<WikiPage> parsedSections = new ArrayList<>();
                        parseSections(parsedPage, parsedSections);

                        for (WikiPage section : parsedSections) {
                            String[] anchorTitles = extractAnchors(section.getPageTitle());

                            for (int i = 0; i < anchorTitles.length; ++i) {
                                context.write(new Text((anchorTitles[i]).toLowerCase())
                                        , new Text(section.getPageText()));
                            }
                        }
                    }
                }
            } catch (ParserConfigurationException | SAXException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Reducer class for @WikiSectionParser. Executes the reducing step.
     */
    public static class WikiSectionReducer extends Reducer<Text, Text , Text, WikiSectionWritable> {
        WikiSectionWritable res;
        ArrayList<Text> redirs;

        /**
         * Reduce step. Removes sections that are nor redirected and joins section texts with all redirect pages which
         * redirect to them.
         * @param key URI of wiki section
         * @param parsedSections parsed sections and redirects to sections
         * @param context hadoop execution context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> parsedSections,
                           Context context
        ) throws IOException, InterruptedException {
            res = new WikiSectionWritable();
            redirs = new ArrayList<>();

            String text = "";

            for (Text section : parsedSections) {
                String sSection = section.toString();
                if (sSection.startsWith(REDIRECTED)) {
                    redirs.add(new Text(sSection.substring(REDIRECTED.length())));
                }
                else {
                    text += sSection;
                }
            }

            if (redirs.size() > 0) {
                String[] keySplits = key.toString().split("#");

                if (keySplits.length < 2)
                    return ;

                res.setPageTitle(new Text(keySplits[0]));
                res.setSectionTitle(new Text(keySplits[1]));

                res.addRedirsFrom(redirs);
                res.setText(new Text(text));

                context.write(key, res);
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = null;
        try {
            parser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = parser.getRemainingArgs();
            if ((remainingArgs.length != 3)) {
                System.err.println("Usage: wiki section parser <in> <out> <num reduce tasks>");
                System.exit(2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        try {
            Job job = Job.getInstance(conf, "wiki section parser");
            job.setJarByClass(WikiSectionParser.class);
            job.setMapperClass(WikiSectionMapper.class);
            job.setReducerClass(WikiSectionReducer.class);
            job.setInputFormatClass(XmlInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(WikiSectionWritable.class);
            job.setNumReduceTasks(Integer.parseInt(args[2]));

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
