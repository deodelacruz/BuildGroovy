package com.isc.tix
import java.text.*
import java.sql.*

class PvcsDeployer extends PvcsBuilder {
	/* Update fields below for deploying a build */	
		String targetEnv = "TEST" //one of LOCAL,DEV,TEST,STAGE,PROD
		String buildIdToDeploy = "280"
		String deploymentDateString = ""	// set to "" to use current time	
		//String deploymentDateString = "08/03/2009 10:00:00 PM"	// set to "" to use current time
	/* */	
	
	File deployScriptFile
	Integer deploymentId = 0	
	Timestamp deploymentDate		
	
	PvcsDeployer() {
		// if deploymentDateString is "", use current date
		if (deploymentDateString.equals("")) {
			deploymentDate = new Timestamp(Calendar.getInstance().getTime().getTime())
		} else {
			SimpleDateFormat df3 = new SimpleDateFormat('MM/dd/yyyy hh:mm:ss a')				
			deploymentDate = new Timestamp(df3.parse(deploymentDateString).getTime())		
		}				
		outputFile = new File(outputDir,"PvcsDeployer." + releaseLabel +  "." + "build_" + buildIdToDeploy + "." + dateString + ".output.txt")	
		errorLogFile = new File(outputDir,"PvcsDeployer." + releaseLabel +  "." + "build_" + buildIdToDeploy + "." + dateString + ".errorlog.txt")		
		deployScriptFile = new File(outputDir,"PvcsDeployer." + releaseLabel +  "." + "build_" + buildIdToDeploy + "." + dateString + ".script.txt")	
	}
	
	/**
	 * main app starting point
	 */
	static void main(args) {
		def dep = new PvcsDeployer()		
		def depOutput = dep.deployByBuildId(dep.buildIdToDeploy,dep.targetEnv, dep.deploymentDate)
		println("DeploymentOutput: ") 
		depOutput.each() {
			dep.tee(dep.outputFile,it.toString())
		} 
		println ("Deployed buildId: " + dep.buildIdToDeploy)
		println("Done. Output written to file " + dep.outputFile.getAbsolutePath())	
		println("Done. Deployment script written to file " + dep.deployScriptFile.getAbsolutePath())
		println("Done. Error messages written to file " + dep.errorLogFile.getAbsolutePath())			
	}
	
	/**
	 * deploy via label using pcli (hmm, this takes a long time)
	 */
	List deployByLabel(String label, String targetEnv) {
		basePcliCmd = "pcli Run -y -ns Get -v${label} -pr${projectRoot} -sp${workspace} -z ${currentProjectPath} "
		tee(outputFile,"basePcliCmd: ${basePcliCmd}")
	    def pcliOutput = basePcliCmd.execute().text //as List
		return pcliOutput.split("\r\n")			
	}
	
	/**
	 * deploy via build id, using revisions stored in the database
	 */
	List deployByBuildId(String buildId, String targetEnv, Timestamp deploymentDate) {
		// peform a get of files to targetEnv
		// get list of revisions for build id from the database
		String workSpace = "/@/RootWorkspace"
		String promotionGroup = "Development"
		
		// generate pcli deployment script, apply a promotion group as well
		println("Getting revisions for build id ${buildId} from the database")
		sql.eachRow("""select PROJECT_DB_NAME, VERSIONED_FILE_PATH, REVISION_NUM
				 		from V_RELEASE_BUILD_REVISION v
				 		where v.BUILD_ID = ?""",[buildId]) {
			workSpace = getWorkSpace(targetEnv,"${it.PROJECT_DB_NAME}")
			promotionGroup = getPromotionGroup(targetEnv,"${it.PROJECT_DB_NAME}")
			tee(deployScriptFile, "Get -sp${workSpace} -pr${repo}/${it.PROJECT_DB_NAME} -t${it.REVISION_NUM} ${it.VERSIONED_FILE_PATH}")
			tee(deployScriptFile, "AssignGroup -g${promotionGroup} -pr${repo}/${it.PROJECT_DB_NAME} -r${it.REVISION_NUM} ${it.VERSIONED_FILE_PATH}")
		}
		
		// run the pcli get command using the deployment script
		errorLogFile.append("\r\n")		
		def pcliCmd =  "${basePcliCmd} Run -y -s${deployScriptFile.getAbsolutePath()} -xe${errorLogFile.getAbsolutePath()} "
		print("pcliCmd: ${pcliCmd}\n");		
		// actual deployment cmd, skip for PROD and STAGE
		pcliOutput = ""
		//if (! targetEnv.equals("PROD") & ! targetEnv.equals("STAGE") ) //{ 
		if (targetEnv.equals("TEST"))  { 			
			pcliOutput = pcliCmd.execute().text //as List
		}
		// if there are no errors, mark the build as having been deployed	
		saveDeploymentToDb(buildId,targetEnv,deploymentDate)
		return pcliOutput.split("\r\n")	
	}
	 
	/**
	 * saves deployment to the database
	 */	
	Integer saveDeploymentToDb(String buildId, String targetEnv, Timestamp deploymentDate) {
		// create a new deployment in the database for given build id, use preparedStmt
		def query = """select id from DEPLOYMENT 
			where build_id = ? and target_env = ? 
			and deployment_date = ? """
		def deploymentId,tmpDeploymentIdRow
		tmpDeploymentIdRow = sql.firstRow(query, [buildId, targetEnv, deploymentDate])
		if (tmpDeploymentIdRow == null) { // do an insert 		
			// get new key for new deployment
			def maxDeploymentId = sql.firstRow("select max(id) maxId from DEPLOYMENT")
			//println("maxDeploymentId: ${maxDeploymentId}")
			if (maxDeploymentId.maxId != null) {
				deploymentId = maxDeploymentId.maxId + 1
			}		
			tee(outputFile,"Inserting into DEPLOYMENT values(${deploymentId},${hibVer},${buildId},${deploymentDate},${targetEnv})")		
			sql.execute("insert into DEPLOYMENT values(?,?,?,?,?)",[deploymentId,hibVer,buildId,deploymentDate,targetEnv])
		} else { //do an update
			deploymentId = tmpDeploymentIdRow.ID
			tee(outputFile,"Update DEPLOYMENT set BUILD_ID = ${buildId}, DEPLOYMENT_DATE = ${deploymentDate}, TARGET_ENV = ${targetEnv} where ID = ${deploymentId}")			
			sql.execute("update DEPLOYMENT set BUILD_ID = ?,DEPLOYMENT_DATE = ?,TARGET_ENV = ? where ID = ?",[buildId,deploymentDate,targetEnv,deploymentId])
		}				
		return deploymentId		
	}
	
	/**
	 * returns the workspace for a given project db and target env
	 */
	//V_ENV_WORKSPACE: ENV_NAME, PROJECT_DB_NAME, ID, VERSION, WORKSPACE_NAME, ENV_ID, PROJECT_DB_ID
	String getWorkSpace(String targetEnv, String projectDb) {		
		def query = "select * from V_ENV_WORKSPACE where lower(ENV_NAME) = ? and lower(PROJECT_DB_NAME) = ? " 
		def sqlParams = [targetEnv.toLowerCase(),projectDb.toLowerCase()]
		def firstRow = sql.firstRow(query,sqlParams)
		if (firstRow != null) {
			return "/@/Public/${firstRow.WORKSPACE_NAME}"
		} else {
			tee(outputFile, "Could not find workspace for ${sqlParams}")
			System.exit(1)
		}
	}
	
	/**
	 * returns the promotion group for a given project db and target env
	 */
	String getPromotionGroup(String targetEnv, String projectDb) {
		def query = "select * from V_ENV_WORKSPACE where lower(ENV_NAME) = ? and lower(PROJECT_DB_NAME) = ? " 
		def sqlParams = [targetEnv.toLowerCase(),projectDb.toLowerCase()]
		def firstRow = sql.firstRow(query,sqlParams)
		if (firstRow != null) {		
			return "${firstRow.PROMOTION_GROUP_NAME}"
		} else {
			tee(outputFile, "Could not find promotion_group for ${sqlParams}")
			System.exit(1)			
		}
	}	
}