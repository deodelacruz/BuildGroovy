package com.isc.tix
import groovy.sql.Sql
import java.sql.*
import  java.text.*

class PvcsPromotionReportImporter {
	def promotionFile
	def outputFile
	def tokens = []
	String driverClass = "oracle.jdbc.driver.OracleDriver" 
	String jdbcUrl = "jdbc:oracle:thin:@req1.tmp.com:1543:REQ1"  
	String dbUser = "*"
	String dbPwd = "*"	 
	def sql,stmt
	Timestamp promotionDate
	
	PvcsPromotionReportImporter() {		
		promotionFile = new File("x:/Temp/tix_pvcs_promotions_120107.prod.csv.txt")
		outputFile = new File("${promotionFile.getAbsolutePath()}.txt")
		// instantiate a jdbc connection immediately		
		sql = Sql.newInstance(jdbcUrl,dbUser,dbPwd,driverClass)				
	}
	
	static void main(args) {
		def imp = new PvcsPromotionReportImporter()
		imp.importPromotionReport()
		//imp.convertSlashes()
		println("Done. Output written to file ${imp.outputFile}.")
	}
	
	void importPromotionReport() {
		def username,action,revisionNum,promotionReportRow
		String archiveLocation,tmpString,tmpArchiveName,archiveName
		def actionTokens = [], sqlParams = []
		SimpleDateFormat df3
		String promotionDateString,query,dateString,timeString
		int i = 0,newKey = -1
		promotionFile.eachLine() {
			//parse each line from file
			tokens = it.split(",")
			archiveLocation = tokens[0].trim()
			// convert p:/pvcs_repository to //vmprojects
			if (archiveLocation.contains("P:\\pvcs_repository\\FanTracker")) {
				tmpString = archiveLocation.substring(29)
				archiveLocation = "\\\\vmprojects\\fantracker${tmpString}"
			}			
			if (archiveLocation.contains("P:\\pvcs_repository\\PublicSideSites")) {
				tmpString = archiveLocation.substring(34)
				archiveLocation = "\\\\vmprojects\\PublicSideSites${tmpString}"
			}
			// convert forward to back slashes
			archiveLocation = archiveLocation.replaceAll("\\\\","/") 			
			tmpArchiveName = (new File(archiveLocation)).getName()
			if (tmpArchiveName.contains(".-ar")) {
				archiveName = tmpArchiveName.substring(0,tmpArchiveName.indexOf(".-ar"))				
			} else {
				archiveName = tmpArchiveName
			}
			username = tokens[1].trim()
			dateString = tokens[2].trim()
			timeString = tokens[3].trim()
			if (timeString.size() == 4) {
				timeString = "00" + timeString
			}
			if (timeString.size() == 2) {
				timeString = "0000" + timeString
			}			
			promotionDateString = "${dateString} ${timeString}"
			df3 = new SimpleDateFormat('yyyyMMdd HHmmss')				
			promotionDate = new Timestamp(df3.parse(promotionDateString).getTime())			
			action = tokens[4].trim()
			actionTokens = action.split()
			revisionNum = actionTokens[1].substring(2)
			tee(outputFile,"${i},${archiveLocation},${archiveName},${revisionNum},${username},${dateString},${timeString},${action},${promotionDate}")
			// if row already exists in db, update values, else insert
			query = """
				select p.ID
				from promotion_report p
				where trim(p.ARCHIVE_LOCATION) = ?
				and trim(p.DATE_STRING) = ?
				and trim(p.ACTION) = ?
			"""
			sqlParams = [archiveLocation,dateString,action]
			tee(outputFile,"${query},${sqlParams}")
			promotionReportRow = sql.firstRow(query,sqlParams)
			try {			
				if (promotionReportRow != null ) { //update record		
					stmt = """
						update PROMOTION_REPORT set PROMOTION_USER = ?,
						DATE_STRING = ?, TIME_STRING = ?, PROMOTION_DATE = ?
						where ID = ?
					"""
					sqlParams = [username,dateString,timeString, promotionDate,promotionReportRow.ID]
					tee(outputFile,"${stmt},${sqlParams}")				
					sql.executeUpdate(stmt,sqlParams)
				} else { // insert record
					query = "select max(p.ID) maxId from promotion_report p"
					newKey = sql.firstRow(query).maxId + 1					
					stmt = "insert into PROMOTION_REPORT values (?,?,?,?,?,?,?,?,?)"
					sqlParams = [newKey,archiveLocation,archiveName,revisionNum,username,dateString,timeString,action,promotionDate]
					tee(outputFile,"${stmt},${sqlParams}")				
					sql.execute(stmt,sqlParams)
				}
			} catch (java.sql.SQLException e) {
				println(e.toString() + ": " + sqlParams)
			}			
			i++
		}
	}
	
	// convert back-slashes to forward-slashes
	void convertSlashes() {
		List sqlParams = ["\\\\vmprojects%"]
		def archiveLocs = []
		//println(System.getProperty("file.separator"))		
		def query = """
			select p.ID, p.ARCHIVE_LOCATION
			from promotion_report p
			where p.ARCHIVE_LOCATION like ?		 
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
			update promotion_report p
			set p.ARCHIVE_LOCATION = ? 
			where	p.id = ?
			"""
			sql.executeUpdate(stmt,sqlParams)
		}
	}
	
	/**
	 * writes string to stdout and file at same time
	 */
	void tee(File outputFile, String string) {	
		 println(string)		 
		 outputFile.append(string + "\r\n")
	}		
}