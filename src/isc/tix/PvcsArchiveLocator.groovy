package com.isc.tix

class PvcsArchiveLocator extends PvcsBuilder {
	List projectDbs = ["publicsidesites"]
	File locatorScriptFile
	List versionedFiles = []
	
	PvcsArchiveLocator() {
		outputFile = new File(outputDir,"PvcsArchiveLocator." + dateString + ".output.txt")
		locatorScriptFile = new File(outputDir,"PvcsArchiveLocator." + dateString + ".script.txt")		
		errorLogFile = new File(outputDir,"PvcsArchiveLocator." + dateString + ".errorlog.txt")	
	}		
	
	static void main(args) {
		def pal = new PvcsArchiveLocator()
		//pal.convertSlashes()
		pal.generateLocatorScript()
		//not needed, pal.runLocatorScript()
		//println("Done. Locator script written to file ${pal.locatorScriptFile}.")
		println("Done. Output written to file ${pal.outputFile}.")		
	}
	
	// get list of versioned files
	void generateLocatorScript() {
		// initially limit to versioned files in promotion report
		/*
		def query = """
			select distinct v.PROJECT_DB_NAME,v.PROJECT_PATH_FILE
			from V_PROJECTPATH_VERSIONEDFILE v,PROMOTION_REPORT p
			where v.ARCHIVE_NAME = p.ARCHIVE_NAME
			and lower(v.PROJECT_DB_NAME) = ?   
			and v.ARCHIVE_LOCATION is null
			order by v.PROJECT_DB_NAME, v.PROJECT_PATH_FILE 
		"""*/
		
		def query = """
			select distinct br.PROJECT_DB_NAME, br.VERSIONED_FILE_PATH PROJECT_PATH_FILE
			from v_release_build_revision br
			where lower(br.PROJECT_DB_NAME) = ?
			and br.RELEASE_LABEL like 'V5.00%'
			and archive_location is null
		"""		
		 
		/*
		def query = """
			select v.PROJECT_DB_NAME,v.PROJECT_PATH_FILE
			from V_PROJECTPATH_VERSIONEDFILE v
			where lower(v.PROJECT_DB_NAME) = ?
			and v.ARCHIVE_LOCATION is null 
		"""		*/
		String scriptLine,archiveLocation, stmt, tmpFileName
		def sqlParams = []
		int i = 0
		tee(outputFile,"${query},${projectDbs}" )
		sql.eachRow(query, projectDbs) {
			// original, generate script file, then run pcli once against entire file
			scriptLine = "getArchiveLocation -pr${repo}/${it.PROJECT_DB_NAME} ${it.PROJECT_PATH_FILE}"
			//versionedFiles.add("${it.PROJECT_DB_NAME},${it.PROJECT_PATH_FILE}") */			
			// new version, run pcli per sql row
			def pcliCmd =  "${basePcliCmd} ${scriptLine}"
			archiveLocation = pcliCmd.execute().text.trim() //as List
			tmpFileName = (new File(it.PROJECT_PATH_FILE)).getName()
			tee(outputFile,"${i},${tmpFileName},${archiveLocation},${it.PROJECT_DB_NAME},${it.PROJECT_PATH_FILE}")				
			if (archiveLocation.contains("[Error]")) {
				archiveLocation = ""
			}
			sqlParams = [archiveLocation,it.PROJECT_DB_NAME,it.PROJECT_PATH_FILE]
			stmt = """
				update versioned_file v
				set v.ARCHIVE_LOCATION = ?
				where v.id = (
				    select f.VERSIONED_FILE_ID
				    from V_PROJECTPATH_VERSIONEDFILE f
				    where f.PROJECT_DB_NAME = ?
				    and f.PROJECT_PATH_FILE = ?
				) 
			"""
			if (archiveLocation.contains(tmpFileName)) {
				tee(outputFile,"${stmt},${sqlParams}")
				sql.executeUpdate(stmt,sqlParams)
			}			
			i++
		}		
	}
	
	/* generate pcli script file of getArchiveLocation commands
	void runLocatorScript() {		
		// run the pcli getArchiveLocation command using the deployment script
		errorLogFile.append("\r\n")		
		def pcliCmd =  "${basePcliCmd} Run -y -s${locatorScriptFile.getAbsolutePath()} -xe${errorLogFile.getAbsolutePath()} "
		print("pcliCmd: ${pcliCmd}\n");		
		pcliOutput = ""
		pcliOutput = pcliCmd.execute().text //as List
		List archiveLocations = pcliOutput.split("\r\n")
		// insert archive loc info from pvcs into db
		def sqlParams = [],tmpTokens = []
		String stmt,projectDb,versionedFile, tmpProjectDb, tmpVersionedFile,tmpFileName
		int i = 0
		archiveLocations.each() {
			tmpTokens = versionedFiles.get(i).split(",")
			tmpProjectDb = tmpTokens[0]
			tmpVersionedFile = tmpTokens[1]
			tmpFileName = (new File(tmpVersionedFile)).getName()
			tee(outputFile,"${i},${tmpFileName},${it},${versionedFiles.get(i)}")			
			sqlParams = [it,tmpProjectDb, tmpVersionedFile]
			stmt = """
				update versioned_file v
				set v.ARCHIVE_LOCATION = ?
				where v.id = (
				    select f.VERSIONED_FILE_ID
				    from V_PROJECTPATH_VERSIONEDFILE f
				    where f.PROJECT_DB_NAME = ?
				    and f.PROJECT_PATH_FILE = ?
				) 
			"""
			if (it.contains(tmpFileName)) {
				sql.executeUpdate(stmt,sqlParams)
			}
			i++
		}
	} */
	
	/**	 
	 * convert back-slashes to forward-slashes
	 */
	void convertSlashes() {
		List sqlParams = ["\\\\vmprojects%"]
		def archiveLocs = []	
		def query = """
			select v.ID, v.ARCHIVE_LOCATION
			from VERSIONED_FILE v
			where v.ARCHIVE_LOCATION like ?	
		"""	
		String tmpArchiveLoc,archiveLoc		
		sql.eachRow(query,sqlParams) {
			tmpArchiveLoc = it.ARCHIVE_LOCATION
			archiveLoc = tmpArchiveLoc.replaceAll("\\\\","/") 
			archiveLocs.add("${archiveLoc},${it.ID}")
		} 
		def stmt, tokens = []
		archiveLocs.each() {
			sqlParams = it.split(",")
			stmt = """
			update VERSIONED_FILE v
			set V.ARCHIVE_LOCATION = ? 
			where v.id = ?
			"""
			tee("${stmt},${sqlParams}")
			sql.executeUpdate(stmt,sqlParams)
		}
	}	
	
}