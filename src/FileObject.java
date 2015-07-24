package dom.gun.ire.minor;

/**
 *
 * @author gagan
 */

public class FileObject {
	int docId;
	int title;
	int content;
	int infobox;
	int outlinks;
	int categories;
	void makezero() {
		docId = -1;
		title = 0;
		content = 0;
		infobox = 0;
		outlinks = 0;
		categories = 0;
	}
	
	public FileObject(){}
	
	public FileObject(String s){
		
	}	
};
