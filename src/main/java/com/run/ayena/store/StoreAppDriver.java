package com.run.ayena.store;

import org.apache.hadoop.util.ProgramDriver;

import com.run.ayena.store.hadoop.HTableObjectMergeMR;
import com.run.ayena.store.hadoop.HTableObjectMergeMR2;
import com.run.ayena.store.hadoop.ObjectDataMR;
import com.run.ayena.store.hadoop.ObjectDfsTool;
import com.run.ayena.store.hbase.ObjectHTableTool;

/**
 * @author Yanhong Lee
 * 
 */
public class StoreAppDriver {

	public static int exec(String[] args) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("htable", ObjectHTableTool.class,
					"A program that process objects via htable.");
			pgd.addClass("dfs", ObjectDfsTool.class,
					"A program that process objects via hdfs.");

			pgd.addClass("object.gen", ObjectDataMR.class,
					"A map/reduce that generate temp objects.");
			pgd.addClass("object.merge", HTableObjectMergeMR.class,
					"A map/reduce that merge temp objects by htable.");
			pgd.addClass("object.merge2", HTableObjectMergeMR2.class,
					"A map/reduce that merge temp objects by htable.");

			pgd.driver(args);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return exitCode;
	}

	public static void main(String[] args) {
		int exitCode = exec(args);
		System.exit(exitCode);
	}

}
