package dom.gun.ire.minor;

import java.io.Serializable;

/**
 *
 * @author gagan
 */
class TermAttributes implements Serializable {

    private int id;
    private int title;
    private int content;
    private int infobox;
    private int outlinks;
    private int categories;
    private int df;

    TermAttributes() {
        id = -1;
        title = 0;
        content = 0;
        infobox = 0;
        outlinks = 0;
        categories = 0;
        df = 0;
    }

    public void increaseTitleCount() {
        title++;
        df++;
    }

    public void increaseContentCount() {
        content++;
        df++;
    }

    public void increaseCategoriesCount() {
        categories++;
        df++;
    }

    public void increaseInfoboxCount() {
        infobox++;
        df++;
    }

    public void increaseOutlinksCount() {
        outlinks++;
        df++;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getContent() {
        return content;
    }

    public int getId() {
        return id;
    }

    public int getInfobox() {
        return infobox;
    }

    public int getOutlinks() {
        return outlinks;
    }

    public int getTitle() {
        return title;
    }

    public int getDf() {
        return df;
    }

    public int getCategories() {
        return categories;
    }
}
