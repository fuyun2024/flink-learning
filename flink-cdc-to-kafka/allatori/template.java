<config>
    <input>
        <jar in="../dataetl-common-1.0.0-SNAPSHOT.jar" out="../dataetl-common-1.0.0-SNAPSHOT.jar"/>
    </input>
    <keep-names>
	<class template="class com.sf.bdp.dataetl.entity.*">
    		<field access="private+"/>
    		<method access="protected+"/>
  	</class>
 	<class template="class com.sf.bdp.dataetl.common.vo.*">
   	 	<field access="private+"/>
    		<method access="protected+"/>
 	</class>
        <class access="protected+">
            <field access="protected+"/>
            <method access="protected+"/>
        </class>   
    </keep-names>
    <property name="log-file" value="log.xml"/>
</config>