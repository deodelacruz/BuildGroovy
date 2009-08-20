package com.isc.tix
import java.text.*

class FileParser {
	String inputFileName = "C:\\svn_work\\branches\\REL_1_00/DeoGroovy\\PcliRunner.0616080242.output.txt"
	File outputFile
	String dateString	
	
	/**
	 * main entry point for script/app
	 */
	static void main(args) {
		FileParser fp = new FileParser()
		fp.dateString = (new SimpleDateFormat('MMddyyhhmm')).format(new Date())
		fp.outputFile = new File("FileParser." + fp.dateString + ".delimited.output.txt" )
	
		fp.splitFileRevisionList().each{
			fp.tee it
		}
		println("Done. Output written to file " + fp.outputFile.getAbsolutePath())				
	}
	
	List splitVersionedFileList() { // comma-separated from project path
		println("| ProjectPath | VersionedFileName")		
		List lines = []
		new File(inputFileName).eachLine{line->
			File vFile = new File(line)
			//String outputLine = vFile.getParent().getAbsolutePath().replace('\\','/') + "," + vFile.getName()
			String outputLine = vFile.getParent().replace('\\','/') + "," + vFile.getName()			
			lines.add(outputLine)
		}		
		return lines
	}
	
	List splitFileRevisionList() { // comma-separated from project path
		println("| ProjectPath | VersionedFileName | FileRevision")				
		List lines = []
		new File(inputFileName).eachLine{line->
			println("line: " + line)		
			File vFile = new File(line)
			String revisionName = vFile.getName()
			String versionedFilePath = vFile.getParent()
			File versionedFile = new File(versionedFilePath)
			String outputLine = versionedFile.getParent().replace('\\','/') + "," + versionedFile.getName() + "," + vFile.getName()			
			lines.add(outputLine)
		}		
		return lines
	}	
	
	/**
	 * returns a list of revisions for a specified label
	 */
	void tee(String string) {	
		 println(string)		 
		 outputFile.append(string + "\r\n")
	}		
	
}