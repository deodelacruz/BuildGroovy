package com.isc.tix
import groovy.sql.Sql

/**
 * class for removing a build and associated records in the db
 */
class PvcsBuildRemover {
	/* Update field below to remove a build from the database. */	 
	List buildIdsToRemove = [279,280,281]
	/* */	 
		
	String driverClass = "oracle.jdbc.driver.OracleDriver" 
	String jdbcUrl = "jdbc:oracle:thin:@req1.tmp.com:1543:REQ1"  
	String dbUser = "*"
	String dbPwd = "*"	 
	def sql,stmt
	
	PvcsBuildRemover() {
			// instantiate a jdbc connection immediately		
			sql = Sql.newInstance(jdbcUrl,dbUser,dbPwd,driverClass)			 
	}
	 
	static void main(args) {
		def pbr = new PvcsBuildRemover()
		Integer buildId
		pbr.buildIdsToRemove.each() {
			buildId = new Integer(it)
			println ("Build removed from the database, buildId: " + pbr.removeBuildFromDb(buildId))
		}
	}
	
	/**
	 * Cascade delete thru foreign key tables
	 */
	Integer removeBuildFromDb(Integer buildId) {
		def statements = []
		// remove change_set from change_set_file_revision
		statements.add("""delete from change_set_file_revision cr
						where cr.CHANGE_SET_ID in (
					    select cs.id
					    from change_set cs
					    where cs.BUILD_ID = ${buildId}
					)""")
		// remove build from table change_set
		statements.add("""delete from change_set cs
					where cs.build_id = ${buildId} """)
		// remove build from table deployment
		statements.add("delete from deployment d where d.build_id = ${buildId}")
		// remove build from table build
		statements.add("delete from build where id = ${buildId}")
		
		// print each sql statement
		statements.each() {
			println("Running sql statement: ${it}" )
			sql.execute(it)
		}
		return buildId
	}
}