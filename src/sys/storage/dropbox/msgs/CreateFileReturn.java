package sys.storage.dropbox.msgs;

public class CreateFileReturn{

	 String path_lower;

	 public void setPath(String path_lower){
	 		this.path_lower=path_lower;
	 }

	 public String getPath(){
	 	return path_lower;
	 }
}