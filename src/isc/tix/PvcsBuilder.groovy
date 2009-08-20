package com.isc.tix
import groovy.sql.Sql
import java.text.*
import java.sql.*

class PvcsBuilder {
	/* Update fields below for creating a new build and applying the labels to a revision */
		String releaseLabel = "V5.0027"
		Integer changeControlId = new Integer(0)		
		String targetReleaseDateString = "09/07/2009 11:00:00 PM"
		String buildDateString = "" // set to "" to use current time	
		List heatCallIds = ["212833","213125"]		
		//List heatCallIds = ["208880","209765","210284","210574","211003","211247","211271","211313","211647","211870","211886"]
	
	/* */
	
	Integer buildId
	List buildRevisions = []
	String repo = "\\\\vmprojects"  // path to ISC's pvcs repository
	String currentProjectDb = "fantracker" // "fantracker", default project db is FanTracker
	String projectRoot = "${repo}/${currentProjectDb}"
	String currentProjectPath = "/" //start archive search from project path
	String idPasswd = ""
	String basePcliCmd = "pcli"
	String pcliOutput
	String outputDir = "x:/scm/logs"
	File outputFile
	File errorLogFile
	File scriptFile
	File buildDocFile
	File masterSqlScriptFile
	String dateString
	Timestamp targetReleaseDate, buildDate
	String driverClass = "oracle.jdbc.driver.OracleDriver" 
	String jdbcUrl = "jdbc:oracle:thin:@req1.tmp.com:1543:REQ1"  
	String dbUser = "*"
	String dbPwd = "*"
	Integer	hibVer = new Integer(0)
	def sql
	
	/**
	 * Constructor		
	 */
	 PvcsBuilder() {
		// instantiate a jdbc connection immediately		
		sql = Sql.newInstance(jdbcUrl,dbUser,dbPwd,driverClass)	
		// set base pcli command
		if (! idPasswd.equals("")) {
	    	basePcliCmd = "${basePcliCmd} -id${idPasswd} "
	    }		
		dateString = (new SimpleDateFormat('MMddyyhhmm')).format(new java.util.Date())		
	}
	 
	/**
	 * main entry point for script/app
	 */
	static void main(args) {
		def p = new PvcsBuilder()
		
		println("PVCS repository Location: " + p.repo)							
		p.buildId = p.createBuild(p.releaseLabel)
		println ("Updated buildId: " + p.buildId)		
		
		// apply release label to heat labeled revisions via pcli
		p.buildRevisions = p.getLabeledRevisions(p.heatCallIds)
		p.applyReleaseLabelToRevisions()
		// store revisions for build in db
		p.saveBuildRevisionsToDb(p.buildId,p.buildRevisions)
		p.generateBuildDocMasterSqlFiles(p.buildId,p.buildRevisions)	
		println ("Updated buildId: " + p.buildId)		
		println("Done. Output written to file " + p.outputFile.getAbsolutePath())	
		println("Done. Labeling script written to file " + p.scriptFile.getAbsolutePath())
		println("Done. Error messages written to file " + p.errorLogFile.getAbsolutePath())		
	}
	
	/**
	 * creates a new build, returns the build id
	 */
	Integer createBuild(String releaseLabel) {

		// get new key for new build
		def maxBuildId = sql.firstRow("select max(id) maxId from BUILD")
		Integer buildId = maxBuildId.maxId + 1				
		
		// set output,err,log files
		outputFile = new File(outputDir,"PvcsBuilder." + releaseLabel +  "." + "build_" + buildId + "." + dateString + ".output.txt")	
		errorLogFile = new File(outputDir,"PvcsBuilder." + releaseLabel + "." + "build_" + buildId + "." + dateString + ".errorlog.txt")
		scriptFile = new File(outputDir,"PvcsBuilder." + releaseLabel + "." + "build_" + buildId + "." + dateString + ".script.txt")
		buildDocFile = new File(outputDir,"PvcsBuilder." + releaseLabel + "." + "build_" + buildId + "." + dateString + ".builddoc.txt")		
		masterSqlScriptFile= new File(outputDir,"PvcsBuilder." + releaseLabel + "." + "build_" + buildId + "." + dateString + ".mastersql.txt")
				
		// get release id matching specified release label from db
		def releaseId = saveReleaseToDb(releaseLabel, targetReleaseDateString)
		// create a new build in the database with given release id
		if (buildDateString.equals("")) {
			buildDate = new Timestamp(Calendar.getInstance().getTime().getTime())
		} else {
			SimpleDateFormat df3 = new SimpleDateFormat('MM/dd/yyyy hh:mm:ss a')				
			buildDate = new Timestamp(df3.parse(buildDateString).getTime())		
		}
		// BUILD TABLE: ID, VERSION, BUILD_DATE, RELEASE_ID		
		String stmt = "insert into BUILD values(?,?,?,?)" 
		def sqlParams = [buildId,hibVer,buildDate,releaseId]
		tee(outputFile,"${stmt},${sqlParams}")
		sql.execute(stmt,sqlParams)				
		return buildId
	}
	
	/**
	 * creates a new build with specified build id and date, returns the build id
	 */
	Integer createBuildWithSpecifiedId(Integer buildId, String buildDateStr, String releaseLabel) {
		SimpleDateFormat df3 = new SimpleDateFormat('MM/dd/yyyy hh:mm:ss a')		
		buildDate = new Timestamp(df3.parse(buildDateStr).getTime())
		println("buildDate: " + buildDate)			
		// get release id matching specified release label from db
		def releaseId = saveReleaseToDb(releaseLabel, targetReleaseDateString)
		// create a new build in the database with given release id
		def tmpId = sql.firstRow("select id from BUILD where id = ?",[buildId])
		if (tmpId == null) { // do an insert 
			// BUILD TABLE: ID, VERSION, BUILD_DATE, RELEASE_ID	
			String stmt = "insert into BUILD values(?,?,?,?)" 
			def sqlParams = [buildId,hibVer,buildDate,releaseId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)				
		} else { //do an update
			String stmt = "update BUILD set BUILD_DATE = ?, RELEASE_ID = ? where ID = ?" 
			def sqlParams = [buildDate,releaseId,buildId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)																	
		}
		return buildId
	}	
	
	/**
	 * save build revisions
	 */
	void saveBuildRevisionsToDb(Integer buildId, List buildRevisions) {
		def heatLabel, revision, revisionNum, prevHeatCallId = 0, heatCallId = 0, versionedFile	
		// sort list of build revisions
		buildRevisions.sort {
			it
		}
		//loop over each build revision line
		buildRevisions.each() {
			//buildRevisionsFormat = heatLabel,projectDb/versionedFile/revisionNum"
			def tokens = it.split(",")
			heatLabel = tokens[0].trim()
			revision = tokens[1].trim()	
			revisionNum = revision.substring(revision.lastIndexOf("/")+1)
			heatCallId = heatLabel.substring(5).trim()
			versionedFile = revision.substring(revision.indexOf("/"),revision.lastIndexOf("/"))
			// get change set id, revision id
			Integer changeSetId = saveChangeSetToDb(buildId,heatCallId)
			def revisionId = saveRevisionToDb(revision)
			// save to db
			String stmt = "select * from CHANGE_SET_FILE_REVISION where change_set_id  = ? and file_revision_id = ?" 
			def sqlParams = [changeSetId,revisionId]
			def tmpId = sql.firstRow(stmt,sqlParams)							
			if (tmpId == null) { // do an insert 
				stmt = "insert into CHANGE_SET_FILE_REVISION values(?,?)" 
				sqlParams = [changeSetId,revisionId]
				tee(outputFile,"${stmt},${sqlParams}")
				sql.execute(stmt,sqlParams)								
			} else { //do an update (optional)
			}
		}
	}	
	
	/**
	 * generate build doc and master sql files
	 */
	 void generateBuildDocMasterSqlFiles(Integer buildId, List buildRevisions) {
			def heatLabel, revision, revisionNum, prevHeatCallId = 0, heatCallId = 0, versionedFile	
			// sort list of build revisions
			buildRevisions.sort {
				it
			}
			//loop over each build revision line
			buildRevisions.each() {
				//buildRevisionsFormat = heatLabel,currentProjectDb/versionedFile/revisionNum"
				def tokens = it.split(",")
				heatLabel = tokens[0].trim()
				revision = tokens[1].trim()	
				revisionNum = revision.substring(revision.lastIndexOf("/")+1)
				heatCallId = heatLabel.substring(5).trim()
				versionedFile = revision.substring(revision.indexOf("/"),revision.lastIndexOf("/"))
				if (versionedFile.contains("control_data/")) { // add TrackerScripts if control_data
					versionedFile = "/TrackerScripts${versionedFile}"
				}
				tee(buildDocFile,"${heatCallId},${versionedFile},${revisionNum}")
				if ((new File(versionedFile)).getName().toLowerCase().indexOf(".sql") != -1) {
					if (! heatCallId.equals(prevHeatCallId)) {
						tee(masterSqlScriptFile,"prompt * ${heatCallId}")				
					}	
					if (versionedFile.contains("pre_grant") || versionedFile.contains("post_grant")) {
					} else {
						tee(masterSqlScriptFile,"prompt * Running: @\"..${versionedFile}\"")
						tee(masterSqlScriptFile,"                  @\"..${versionedFile}\"")
					}
					prevHeatCallId = heatCallId				
				}
			}
	}
	
	/**
	 * returns the change set id for given build id and heat id from db
	 */
	Integer getChangeSetId(Integer buildId, String heatCallId) {		
		if (heatCallId.toLowerCase().indexOf("heat_") == 0) {
			heatCallId = heatCallId.substring(4)
		}		
		def match = sql.firstRow("select id from CHANGE_SET where BUILD_ID = ? and HEAT_CALL_ID = ? ",[buildId,heatCallId] )		
		if (match == null) {
			return new Integer(-1)
		} else {
			return match.id
		}
	}
	
	/**
	 * saves a change set to db, and returns the change set id
	 */
	Integer saveChangeSetToDb(Integer buildId, String heatCallId) {
		def changeSetId = getChangeSetId(buildId,heatCallId)
		if (changeSetId < 0) { 	// changeSetId not yet in db, add it
			def maxChangeSetId = sql.firstRow("select max(id) maxId from CHANGE_SET")
			def newKey = maxChangeSetId.maxId + 1		
			//CHANGE_SET TABLE: ID, VERSION, BUILD_ID, HEAT_CALL_ID				
			def stmt = "insert into CHANGE_SET values(?,?,?,?)" 
			def sqlParams = [newKey,hibVer,buildId,heatCallId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)						
			changeSetId = getChangeSetId(buildId,heatCallId)						
		} 
		return changeSetId	
	}
	
	/**
	 * returns release id for given label
	 */
	Integer getReleaseId(String releaseLabel) {
		def stmt =  "select * from RELEASE r where r.RELEASE_LABEL = ?"
		def sqlParams = [releaseLabel]		
		tee(outputFile,"${stmt},${sqlParams}")
		def match = sql.firstRow(stmt,sqlParams)		
		if (match == null) {
			return new Integer(-1)
		} else {
			return match.ID
		}
	}	
	
	/**
	 * saves a release label into the db and returns the release id
	 */
	Integer saveReleaseToDb(String releaseLabel, String targetReleaseDateString) {		
		SimpleDateFormat df = new SimpleDateFormat('MM/dd/yyyy hh:mm:ss a')  // "06/15/2008 10:28:00 PM"
		targetReleaseDate = new Timestamp((df.parse(targetReleaseDateString)).getTime())			
		def releaseId = getReleaseId(releaseLabel)
		if (releaseId < 0) { 	// release not yet in db, add it
			// get new key for new release
			def maxReleaseId = sql.firstRow("select max(id) maxId from RELEASE")
			releaseId = maxReleaseId.maxId + 1		
			String relDateString = """to_date('${targetReleaseDate}','MM/DD/YYYY HH:MI:SS PM')""" 
			//RELEASE TABLE: ID, VERSION, RELEASE_LABEL, TARGET_RELEASE_DATE							
			String stmt = "insert into RELEASE values(?,?,?,?,?)" 
			def sqlParams = [releaseId,hibVer,releaseLabel,targetReleaseDate,changeControlId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)		
			releaseId = getReleaseId(releaseLabel)			
		} else {	// update target date of release					
			String stmt = "update RELEASE set TARGET_RELEASE_DATE = ? where ID = ?" 
			def sqlParams = [targetReleaseDate,releaseId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)							
		}
		return releaseId
	}
	
	/**
	 * returns a list of project databases in specified repository
	 */
	List getProjectDbs(String repositoryPath) {
	   String listCmd =  "${basePcliCmd} List -pr\"${repo}\\${currentProjectDb}\" -tProject -l -z";
	    listCmd = "${listCmd}" + " ${currentProjectPath}* ";    
	    print("listCommand: ${listCmd}\n");		
	    pcliOutput = listCmd.execute().text //as List
		return pcliOutput.split("\r\n")	    	
	}
	
	/**
	 * returns a list of project paths in specified repository
	 */
	List getProjectPaths(String repositoryPath) {
	   String listCmd =  "${basePcliCmd} List -pr\"${repo}\\${currentProjectDb}\" -tProject -l -z";
	    listCmd = "${listCmd}" + " ${currentProjectPath}* ";    
	    print("listCommand: ${listCmd}\n");		
	    pcliOutput = listCmd.execute().text //as List
		return pcliOutput.split("\r\n")	    	
	}
	
	/**
	 * returns a list of versioned files in specified project path 
	 */
	List getVersionedFiles(String projectDb, String projectPath) {
	   def pcliCmd =  "${basePcliCmd} ListVersionedFiles -pr\"${repo}\\${projectDb}\" -l -z ${projectPath}* " 
	    print("pcliCmd: ${pcliCmd}\n");		
	    pcliOutput = pcliCmd.execute().text //as List
		return pcliOutput.split("\r\n")	    	
	}	
	
	/**
	 * returns a list of versioned files in format: projectPath,fileName 
	 */
	List getPathVersionedFiles(String projectDb, String projectPath) {
	   def pcliCmd =  "${basePcliCmd} ListVersionedFiles -pr\"${repo}\\${projectDb}\" -l -z ${projectPath}* " 
	    print("pcliCmd: ${pcliCmd}\n");		
	    pcliOutput = pcliCmd.execute().text //as List
		return pcliOutput.split("\r\n")	 
		getVersionedFiles(projectDb, projectPath).each{
	    	(new File(it)).getParent() + "," + it
	    }
	}		

	/**
	 * returns a list of file revisions under project path
	 */	    
	List getFileRevisions(String projectPath) {
		    def pcliCmd =  "${basePcliCmd} List -pr\"${repo}\\${currentProjectDb}\" "
			pcliCmd = "${pcliCmd}" + "-l -zt -ntProject -ntVersionedFile -aArchiveRevision ${projectPath}* " 
		    print("pcliCmd: ${pcliCmd}\n");		
		    pcliOutput = pcliCmd.execute().text //as List
			return pcliOutput.split("\r\n")	 					
	}
	
	/**
	 * 
	 */
	List getRevisionAttributes(projectPath) {
		   def pcliCmd =  "${basePcliCmd} List -pr\"${repo}\\${currentProjectDb}\" "
			pcliCmd = "${pcliCmd}" + "-l -zt -ntProject -ntVersionedFile -aArchiveRevision -aArchiveRevision:CheckedInDate -aArchiveRevision:Author "
			pcliCmd = "${pcliCmd}" + " -aArchiveRevision:Labels -aArchiveRevision:PromotionGroups -aArchiveRevision:ChangeDescription ${projectPath}* " 
		    print("pcliCmd: ${pcliCmd}\n");		
		    pcliOutput = pcliCmd.execute().text //as List
			return pcliOutput.split("\r\n")	
	}
			
	/**
	 * writes string to stdout and output file at same time
	 */
	void tee(String string) {	
		 println(string)		 
		 outputFile.append(string + "\r\n")
	}	
	
	/**
	 * writes string to stdout and specified file at same time
	 */
	void tee(File outputFile, String string) {	
		 println(string)		 
		 outputFile.append(string + "\r\n")
	}	
	
	/**
	 * returns label id from label table 
	 */
	Integer getLabelId(String label) {
		def labelMatch = sql.firstRow("select id from label l where l.LABEL_NAME = ?",[label])		
		if (labelMatch == null) {
			return new Integer(-1)
		} else {
			return labelMatch.id
		}
	}
	
	/**
	 * returns revision id from revision view 
	 */
	Integer getRevisionId(String revision) {
		// revision string sample: fantracker/CentralAdmin/AdminLauncher.cfm/1.5
		String stmt = "select * from V_VERSIONEDFILE_REVISION r where r.DB_REVISION_PATH = ?" 
		def sqlParams = [revision]
		tee(outputFile,"${stmt},${sqlParams}")
		def revMatch =	sql.firstRow(stmt,sqlParams)
		if (revMatch == null) {
			return new Integer(-1)
		} else {
			return revMatch.REVISION_ID
		}
	}	
	
	/**
	 * inserts a revision into the db and returns the new revision id
	 */
	Integer saveRevisionToDb(String revision) {
		// revision string sample: fantracker/CentralAdmin/AdminLauncher.cfm/1.5		
		def revisionId = getRevisionId(revision)
		def sqlParams = []
		if (revisionId < 0) { 	// revision not yet in db, add it
			//get versioned file id, if not present, add it to db
			Integer versionedFileId = getVersionedFileIdOfRevision(revision)
			if (versionedFileId < 0) { //versioned file not in db yet, do an insert
		 		String versionedFilePath = getVersionedFilePathOfRevision(revision)				
				versionedFileId = saveVersionedFileToDb(versionedFilePath)						
			}
			def maxRevisionId = sql.firstRow("select max(id) maxId from FILE_REVISION")
			def newKey = maxRevisionId.maxId + 1		
			String revisionText = revision.substring(revision.lastIndexOf("/")+1)
			//FILE_REVISION TABLE: ID, VERSION, CHANGE_DESC, CHECK_IN_DATE, CHECK_IN_USER, REVISION, VERSIONED_FILE_ID
			String stmt = "insert into FILE_REVISION values(?,?,?,?,?,?,?)" 
			sqlParams = [newKey,hibVer,null,null,null,revisionText,versionedFileId]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams) 
			// revisionId = getRevisionId(revision)
			revisionId = newKey
		} 
		return revisionId		
	}
	
	Integer getVersionedFileIdOfRevision(String revision) {		
		String stmt = "select * from V_VERSIONEDFILE_REVISION where REVISION_PATH = ?" 
		def sqlParams = [revision]
		tee(outputFile,"${stmt},${sqlParams}")
		def match =	sql.firstRow(stmt,sqlParams)		
		if (match == null) {
			return new Integer(-1)
		} else {
			return match.VERSIONED_FILE_ID
		}		
	}
	
	String getVersionedFilePathOfRevision(String revision) {
		return revision.substring(0,revision.lastIndexOf("/"))		
	}
	
	/**
	 * returns versioned file id 
	 */
	Integer getVersionedFileId(String versionedFile) {
		def sqlParams = [versionedFile]
		String query = "select * from V_PROJECTPATH_VERSIONEDFILE where DB_PROJECT_PATH_FILE = ?"
		tee(outputFile,"${query},${sqlParams}")
		def match = sql.firstRow(query, sqlParams)
		if (match == null) {
			return new Integer(-1)
		} else {
			return match.VERSIONED_FILE_ID
		}
	}
	
	Integer saveVersionedFileToDb(String versionedFile) {
		def versionedFileId = getVersionedFileId(versionedFile)
		if (versionedFileId < 0) { 	// check if versioned file already in db, if not, add it
			// get projecpathid, if not present, add it to db
	 		Integer projectPathId = getProjectPathIdOfVersionedFile(versionedFile)	
			if (projectPathId < 0) {	//projectpath not in db yet, do an insert
		 		String projectPath = getProjectPathOfVersionedFile(versionedFile)				
				projectPathId = saveProjectPathToDb(projectPath)
			}
			def maxVersionedFileId = sql.firstRow("select max(id) maxId from VERSIONED_FILE")
			def newKey = maxVersionedFileId.maxId + 1
			String archiveName = versionedFile.substring(versionedFile.lastIndexOf("/")+1)
			//VERSIONED_FILE TABLE: ID, VERSION, ARCHIVE_NAME, PROJECT_PATH_ID	
			def sqlParams = [newKey,hibVer,archiveName,projectPathId,null]
			String stmt = "insert into VERSIONED_FILE values(?,?,?,?,?)" 
			tee(outputFile,"${stmt},${sqlParams}")			
			sql.execute(stmt, sqlParams) 
			//versionedFileId = getVersionedFileId(versionedFile)	
			return newKey
		} 
		return versionedFileId
	}
	
	String getProjectPathOfVersionedFile(String versionedFile) {
		return versionedFile.substring(0,versionedFile.lastIndexOf("/"))
	}
	
	Integer getProjectPathIdOfVersionedFile(String versionedFile) {
		def match = sql.firstRow("select * from	V_PROJECTPATH_VERSIONEDFILE where DB_PROJECT_PATH_FILE = ?",[versionedFile])
		if (match == null) {
			return new Integer(-1)
		} else {
			return match.PROJECT_PATH_ID
		}		
	}
	
	/**
	 * returns projectpath_id matching given projectpath
	 */
	Integer getProjectPathId(String projectPath) {
		def pathMatch = sql.firstRow("select * from V_PROJECT_DB_PATH where db_project_path = ?", [projectPath])
		if (pathMatch == null) {
			return new Integer(-1)
		} else {
			return pathMatch.PROJECT_PATH_ID
		}		
	}	

	/**
	 * 
	 */
	Integer saveProjectPathToDb(String projectPath) {
		def projectPathId = getProjectPathId(projectPath)
		if (projectPathId < 0) {
			def projectDbId = getProjectDbIdOfProjectPath(projectPath)	
			if (projectDbId < 0) {	//projectdb not in db yet, do an insert
				projectDbId = saveProjectDbToDb(getProjectDbNameFromProjectPath(projectPath))
			}
			def maxProjectPathId = sql.firstRow("select max(id) maxId from PROJECT_PATH")
			def newKey = maxProjectPathId.maxId + 1
			String projectPathNoDb = getProjectPathNoDb(projectPath)
			//PROJECT_PATH TABLE: ID, VERSION, PROJECT_DB_ID, PROJECT_PATH_NAME	
			String stmt = "insert into PROJECT_PATH values(?,?,?,?)" 
			def sqlParams = [newKey,hibVer,projectDbId,projectPathNoDb]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)										
			projectPathId = newKey
		}
		return projectPathId
	}	

	/**
	 * returns project path with no db from project path
	 */
	String getProjectPathNoDb(String projectPath) {
		def slashIndex = projectPath.indexOf("/")
		if (slashIndex > 0) {		
			return projectPath.substring(slashIndex)
		} else {
			return "/"
		}
	}	
	
	/**
	 * returns project db name from project path
	 */
	String getProjectDbNameFromProjectPath(String projectPath) {
		def slashIndex = projectPath.indexOf("/")
		if (slashIndex > 0) {
			return projectPath.substring(0,slashIndex)
		} else {
			return projectPath
		}
	}

	/**
	 * returns projectdb_id matching given projectdb name
	 */
	Integer getProjectDbIdOfProjectPath(String projectPath) {
		def dbMatch = sql.firstRow("""select p.PROJECT_DB_ID from V_PROJECT_DB_PATH p 
										where p.DB_PROJECT_PATH = ?""", [projectPath])
		if (dbMatch == null) {
			return new Integer(-1)
		} else {
			return dbMatch.PROJECT_DB_ID
		}		
	}	
	
	/**
	 * returns projectdb_id matching given projectdb name
	 */
	Integer getProjectDbId(String projectDb) {
		def dbMatch = sql.firstRow("select * from PROJECT_DB where PROJECT_DB_NAME = ?", [projectDb])
		if (dbMatch == null) {
			return new Integer(-1)
		} else {
			return dbMatch.ID
		}		
	}
	
	/**
	 * inserts new projectdb in the database and returns the projectdb id
	 */
	Integer saveProjectDbToDb(String projectDb) {
		def projectDbId = getProjectDbId(projectDb) 
		if (projectDbId < 0) { //not yet in project db, add it to db
			def maxProjectDbId = sql.firstRow("select max(id) maxId from PROJECT_DB")		
			def newKey = maxProjectDbId.maxId + 1
			String stmt = "insert into PROJECT_DB values(?,?,?)" 
			def sqlParams = [newKey,hibVer,projectDb]
			tee(outputFile,"${stmt},${sqlParams}")
			sql.execute(stmt,sqlParams)							
			projectDbId = getProjectDbId(projectDb)
		}
		return projectDbId
	}
	
	/**
	 * returns a list of revisions for given labels
	 */
	List getLabeledRevisions(List heatCallIds) {
		// convert heatCallIds to heatlabels
		//run pcli command
		def pcliCmd =  "${basePcliCmd} ListRevision -pr\"${projectRoot}\" "
		heatCallIds.each() {
			def heatLabel = "heat_${it}"        
			print("Label: ${heatLabel}\n")
			pcliCmd = "${pcliCmd} -v${heatLabel}"       
	    }    
		pcliCmd = "${pcliCmd} -z '${currentProjectPath}'" 
	    print("pcliCmd: ${pcliCmd}\n")
		pcliOutput = pcliCmd.execute().text //as List
		List tmpRevisions = pcliOutput.split("\r\n")
		List revisions = []
		List tokensByTab 
		String versionedFile 
		List labelRevision 
		def heatLabel, revision, tmpTokens 
		//def revision 
		String line 		
		tmpRevisions.each() {
			tokensByTab = it.toString().split("\\t+")		
			List tokens = []
			tokensByTab.each() {
				if (! it.trim().equals("")) tokens.add(it.trim())
			}
			versionedFile = tokens[0]
			// in case of multiple labels, get higher revision only
			labelRevision = tokens[1].split(":=")
			if (labelRevision.size() > 2) {
				tee("Warning, ${versionedFile} is touched by multiple heat labels for this release: ")
				tee(it)
			}
			heatLabel = labelRevision[0].trim()
			tmpTokens = labelRevision[1].split()
			revision = tmpTokens[0].trim()
			line = "${heatLabel},${currentProjectDb}${versionedFile}/${revision}"
			revisions.add(line)
			//tee(line)
			tee(scriptFile,"Label -pr${projectRoot} -r${revision} -v${releaseLabel} ${versionedFile}")
		}		
		return	revisions    
	}
	
	/**
	 * read a pcli label script file and applies label
	 */
	List applyReleaseLabelToRevisions() {	
		// remove release label from all current revisions first
		/* def delPcliCmd = "${basePcliCmd} DeleteLabel -pr${projectRoot} -v${releaseLabel} -z /"
		tee(outputFile,"delPcliCmd: ${delPcliCmd}")	
	    pcliOutput = delPcliCmd.execute().text //as List	
		tee(outputFile,delPcliCmd) */
		// reapply release label to new revisions
		errorLogFile.append("\r\n")			
		def pcliCmd =  "${basePcliCmd} run -y -s${scriptFile.getAbsolutePath()} -xe${errorLogFile.getAbsolutePath()} "
		tee(outputFile,"pcliCmd: ${pcliCmd}")		
	    pcliOutput = pcliCmd.execute().text //as List
		return pcliOutput.split("\r\n")			
	}
	
}