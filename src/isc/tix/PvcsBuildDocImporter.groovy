package com.isc.tix
import java.text.*
import java.sql.*

class PvcsBuildDocImporter extends PvcsDeployer {
	/* Update fields below for importing data from a build doc */		
	File buildDocFile = new File("X:/scm/buildDocs/2005/Build_20060109_v40073_prod.csv.txt")
	Integer buildId = new Integer(40)
	def deploymentId = 0
	String buildDateString = "01/09/2006 02:00:00 PM"	
	/* */
	
	String releaseLabel	
	Timestamp buildDate, deploymentDate 
	String targetEnv

	PvcsBuildDocImporter() {
		outputFile = new File(outputDir,"PvcsBuildDocImporter." + dateString + ".output.txt")	
	}	
	
	/**
	 * main application logic, imports build data from a build doc file
	 */
	static void main(args) {
		def i = new PvcsBuildDocImporter() 		
		i.tee(i.outputFile,"Importing build doc data from ${i.buildDocFile} to buildId: ${i.buildId}")		
		i.parseBuildDocFileName()
		i.buildId = i.createBuildWithSpecifiedId(i.buildId, i.buildDateString, i.releaseLabel)
		i.importBuildDocData()
		println ("Imported build doc data from ${i.buildDocFile} to buildId: ${i.buildId}")
		println("Done. Output written to file " + i.outputFile.getAbsolutePath())	
	}
	
	/**
	 * gets release info from build doc file name
	 */
	void parseBuildDocFileName() {
		List tokens = buildDocFile.getName().split("_")
		println("fileNameTokens: " + tokens)
		
		// get deployment date
		SimpleDateFormat df1 = new SimpleDateFormat('yyyyMMdd')
		Timestamp tmpDate = new Timestamp((df1.parse(tokens[1])).getTime())		
		SimpleDateFormat df2 = new SimpleDateFormat('MM/dd/yyyy')		
		def deploymentDateString = df2.format(tmpDate) + " 11:00:00 pm"
		tee(outputFile,"deploymentDateString: " + deploymentDateString)
		SimpleDateFormat df3 = new SimpleDateFormat('MM/dd/yyyy hh:mm:ss a')		
		deploymentDate = new Timestamp(df3.parse(deploymentDateString).getTime())
		tee(outputFile,"deploymentDate: " + deploymentDate)		
		buildDate = new Timestamp(df3.parse(buildDateString).getTime())		
		tee(outputFile,"buildDate: " + buildDate)		
		
		// get release label
		def tmpLabel = tokens[2]
		releaseLabel = tmpLabel.substring(0,2).toUpperCase() + "." +  tmpLabel.substring(2)
		tee(outputFile,"releaseLabel: " + releaseLabel)
		
		// get target env
		def remainder = tokens[3].split("\\.")
		//println("remainder: " + remainder)		
		targetEnv = remainder[0].toUpperCase()
		tee(outputFile,"targetEnv: " + targetEnv)
	}
	
	/**
	 *  read data from file
	 */
	void importBuildDocData() {
		def tokens = [], tmpTokens = [], buildRevisions = []
		def heatLabel,projectDb,versionedFilePath,revisionNum,revisionPath,buildRevision		
		def buildDocLines = []
		buildDocFile.eachLine {
			buildDocLines.add(it)
		}
		//parse each line
		// convert lines to build revisions list format usable by saveBuildRevisionsToDb method
		buildDocLines.each {
			tokens = it.split(",")
			heatLabel = tokens[0]
			if (heatLabel.indexOf("/")) {
				tmpTokens = heatLabel.split("/")
				heatLabel = tmpTokens[0]
			}
			if (heatLabel.indexOf("heat_") == -1) {
				heatLabel = "heat_${heatLabel}"
			}			
			if (heatLabel.equals("Master SQL File")) {
				heatLabel = "heat_000000"
			}
			projectDb =  tokens[3]
			if (projectDb.equals("FanTracker") || projectDb == null) {
				projectDb = "fantracker"
			}
			versionedFilePath = tokens[1]		
			if (! versionedFilePath.substring(0,1).equals("/")) {
				versionedFilePath = "${projectDb.trim()}/${versionedFilePath.trim()}"
			} else {
				versionedFilePath = "${projectDb}${versionedFilePath}"				
			}
			revisionNum = tokens[2]
			revisionPath = "${versionedFilePath}/${revisionNum}"			
			buildRevision = "${heatLabel},${revisionPath}"	
			//println("buildRevision: " + buildRevision)		
			buildRevisions.add(buildRevision)		
		}
		buildRevisions.each {
			tee(outputFile,it)
		}
		println("====")
		// save build revisions list to the db
		saveBuildRevisionsToDb(buildId,buildRevisions)
		//def depOutput = dep.deployByBuildId(dep.buildIdToDeploy,dep.targetEnv)
		deployByBuildId("${buildId}",targetEnv, deploymentDate)
	}	 
}