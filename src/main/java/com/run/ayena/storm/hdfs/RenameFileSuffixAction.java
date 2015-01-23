package com.run.ayena.storm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RenameFileSuffixAction implements RotationAction {

	private static final long serialVersionUID = -271660086322082841L;

	private static final Logger LOG = LoggerFactory
			.getLogger(MoveFileAction.class);

	private String src;
	private String dst;

	public RenameFileSuffixAction change(String src, String dst) {
		this.src = src;
		this.dst = dst;
		return this;
	}

	@Override
	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {
		String name = filePath.getName();
		int idx = name.lastIndexOf(src);
		if (idx > 0) {
			name = name.substring(0, idx);
		}
		name = name + dst;

		Path destPath = new Path(filePath.getParent(), name);
		boolean success = fileSystem.rename(filePath, destPath);
		LOG.info("Move file {} to {} : " + success, filePath, destPath);
		return;
	}

}
