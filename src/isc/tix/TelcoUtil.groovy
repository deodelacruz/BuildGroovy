package com.isc.tix
import groovy.sql.Sql
//import java.text.*
import java.sql.*

class TelcoUtil {
		String driverClass = "oracle.jdbc.OracleDriver" 
		String jdbcUrl = "jdbc:oracle:thin:@req1.tmp.com:1543:REQ1"  
		String dbUser = "**"
		String dbPwd = "**"	
		def sql
	
		TelcoUtil() {
			// instantiate a jdbc connection immediately		
			sql = Sql.newInstance(jdbcUrl,dbUser,dbPwd,driverClass)		
		}
		
	static void main(args) {
		def u = new TelcoUtil()
		u.runTask()
		println("Done.")
	}
	
	void runTask() {
		String stmt
		def sqlParams = []
		def sql2 = Sql.newInstance(jdbcUrl,dbUser,dbPwd,driverClass)
		sql.eachRow("select * from vendor_contacts") {
			stmt = "update vendor_contacts set location_id = ? where vendorcontactsid = ?"
			sqlParams = [it.locationid, it.vendorcontactsid]
			println("Updating ${it.vendorcontactsid}")
			sql2.executeUpdate(stmt, sqlParams) 
			
		}
	}
}