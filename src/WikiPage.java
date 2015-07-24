package dom.gun.ire.minor;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author gagan
 */
public class WikiPage {

    private StringBuilder title;
    private int id;
    private StringBuilder content;
    private ArrayList<StringBuilder> infobox;
    private ArrayList<StringBuilder> outlinks;
    private ArrayList<StringBuilder> categories;

    public WikiPage() {
        infobox = new ArrayList<StringBuilder>();
        outlinks = new ArrayList<StringBuilder>();
        categories = new ArrayList<StringBuilder>();
    }

    public void setContent(StringBuilder content) {
        content = parseContent(content);

        this.content = content;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTitle(StringBuilder title) {
        title = removeNonAsciiAndNumbers(title);
        this.title = title;
    }

    public StringBuilder getContent() {
        return content;
    }

    public int getId() {
        return id;
    }

    public StringBuilder getTitle() {
        return title;
    }

    public ArrayList<StringBuilder> getCategories() {
        return categories;
    }

    public ArrayList<StringBuilder> getInfobox() {
        return infobox;
    }

    public ArrayList<StringBuilder> getOutlinks() {
        return outlinks;
    }

    public void setCategories(ArrayList<StringBuilder> Categories) {
        this.categories = Categories;
    }

    public void setInfobox(ArrayList<StringBuilder> infobox) {
        this.infobox = infobox;
    }

    public void setOutlinks(ArrayList<StringBuilder> outlinks) {
        this.outlinks = outlinks;
    }

    private StringBuilder parseContent(StringBuilder content) {

        content = removeNonAsciiAndNumbers(content);

        // rfetches category
        String startPattern = "[[Category:";
        String endPattern = "]]";
        String bugPattern = "[[";

        generateSubContent(content, startPattern, endPattern, bugPattern, FieldType.categories);

        // remove everything after See Also
        removeSeeAlso(content);

        startPattern = "{{Infobox";
        endPattern = "}}";
        bugPattern = "{{";

        generateSubContent(content, startPattern, endPattern, bugPattern, FieldType.infobox);


        startPattern = "[[Image";
        endPattern = "]]";
        bugPattern = "[[";

        generateSubContent(content, startPattern, endPattern, bugPattern, FieldType.noval);

        // removing [[ : ]]
        removeExtraOutlink(content);

        startPattern = "[[";
        endPattern = "]]";
        bugPattern = "[[";

        generateSubContent(content, startPattern, endPattern, bugPattern, FieldType.outlinks);

        removeExtraData(content);

        return content;
    }

    private void removeSeeAlso(StringBuilder content) {

        String s = "==See also==";
        int length = content.length();
        int startIndex = content.indexOf(s);

        if (startIndex != -1) {
            content = content.delete(startIndex, length - 1);
        }
    }

    private void removeExtraOutlink(StringBuilder content) {
        Pattern p = Pattern.compile("\\[\\[.*:.*\\]\\]");
        Matcher m = p.matcher(content);

        content = new StringBuilder(m.replaceAll(""));
    }

    private StringBuilder removeNonAsciiAndNumbers(StringBuilder content) {
        Pattern p = Pattern.compile("[^\\x00-\\x7F]");
        Matcher m = p.matcher(content);

        content = new StringBuilder(m.replaceAll(""));

        // removing numbers
        p = Pattern.compile("[0-9]+");
        m = p.matcher(content);

        content = new StringBuilder(m.replaceAll(""));

        return content;
    }

    private void removeExtraData(StringBuilder content) {

        // removing {{ }}
        int startIndex = content.indexOf("{{");
        int endIndex;

        while (startIndex != -1) {
            endIndex = content.indexOf("}}", startIndex + 2);
            if (endIndex == -1) {
                break;
            }
            content.delete(startIndex, endIndex + 2);

            startIndex = content.indexOf("{{", startIndex);
        }


        // remove html comments
        startIndex = content.indexOf("<!--");

        while (startIndex != -1) {
            endIndex = content.indexOf("-->", startIndex + 4);
            if (endIndex == -1) {
                break;
            }

            content.delete(startIndex, endIndex + 3);

            startIndex = content.indexOf("<!--", startIndex);
        }

        // remove html links
        startIndex = content.indexOf("http://");
        int oldStartIndex = 0;

        while (startIndex != -1 && startIndex < content.length() && oldStartIndex != startIndex) {
            endIndex = content.indexOf(" ", startIndex + 4);
            if (endIndex == -1) {
                endIndex = content.indexOf("\n", startIndex + 4);
            }

            if (endIndex != -1) {
                content.delete(startIndex, endIndex + 1);
            }

            oldStartIndex = startIndex;

            startIndex = content.indexOf("http://", startIndex);
        }
    }

    private void removePipedContentsofoutlinks(StringBuilder content) {
        int endIndex = content.indexOf("|");
        if (endIndex != -1) {
            content.delete(0, endIndex);
        }
    }

    private void generateSubContent(StringBuilder content, String startPattern, String endPattern,
            String bugPattern, FieldType tag) {

        int startPatternIndex = content.indexOf(startPattern);

        while (startPatternIndex != -1) {
            int endPatternIndex = content.indexOf(endPattern, startPatternIndex + startPattern.length());
            int bugPatternIndex = content.indexOf(bugPattern, startPatternIndex + startPattern.length());

            while (bugPatternIndex != -1 && bugPatternIndex < endPatternIndex) {
                endPatternIndex = content.indexOf(endPattern, endPatternIndex + endPattern.length());
                bugPatternIndex = content.indexOf(bugPattern, bugPatternIndex + bugPattern.length());
            }


            StringBuilder sb = new StringBuilder();

            if (startPatternIndex != -1 && endPatternIndex != -1) {
                sb.append(content.substring(startPatternIndex + startPattern.length(), endPatternIndex));
            }


            switch (tag) {
                case infobox:
                    generateSubContent(sb, "[[", "]]", "[[", FieldType.outlinks);
                    removeExtraData(sb);
                    infobox.add(sb);
                    break;
                case categories:
                    removeExtraData(sb);
                    categories.add(sb);
                    break;
                case outlinks:
                    generateSubContent(sb, "[[", "]]", "[[", FieldType.outlinks);
                    removePipedContentsofoutlinks(sb);
                    removeExtraData(sb);
                    outlinks.add(sb);
                    break;
                case noval:
                    // don't add to anything
                    break;
            }

            if (startPatternIndex != -1 && endPatternIndex != -1 && tag != FieldType.outlinks) {
                content.delete(startPatternIndex, endPatternIndex + endPattern.length());
            }

            startPatternIndex = content.indexOf(startPattern, startPatternIndex + startPattern.length());
        }
    }
}
