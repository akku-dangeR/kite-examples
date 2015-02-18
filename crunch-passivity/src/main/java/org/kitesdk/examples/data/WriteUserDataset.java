/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.examples.data;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * Write some user objects to the users dataset using Avro specific records.
 */
public class WriteUserDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

	  URI datasetUri = URI.create("dataset:hdfs:test_crunch_passivity/users");
	  Dataset<User> userDataset = null;
	  if (!Datasets.exists(datasetUri)) {
		  DatasetDescriptor datasetDescriptor = new DatasetDescriptor.Builder().schema(User.SCHEMA$).build();
		  userDataset = Datasets.create(datasetUri, datasetDescriptor, User.class);
	  } else {
		  userDataset = Datasets.load(datasetUri, User.class);
		  userDataset = Datasets.update(datasetUri, 
				  new DatasetDescriptor.Builder(userDataset.getDescriptor()).schema(User.SCHEMA$).build(), User.class);
	  }
	  
	  Path usersPath = new Path("/tmp/crunch_passivity/" + System.currentTimeMillis() + "/users/users.avro");
	  writeUsersToHDFS(usersPath);
	  
	  Pipeline pipeline = new MRPipeline(getClass(), getConf());
	  PCollection<User> users = pipeline.read(From.avroFile(usersPath, User.class));
	  users.write(CrunchDatasets.asTarget(userDataset), Target.WriteMode.APPEND);
	  PipelineResult result = pipeline.done();
	  
	  if (result != null && PipelineExecution.Status.SUCCEEDED == result.status.SUCCEEDED) {
		  return 0;
	  }
	  
	  throw new RuntimeException("Failure writing to dataset with uri: " + datasetUri.toString());
  }

  
  private void writeUsersToHDFS(Path path) throws IOException {
	  FileSystem fs = FileSystem.get(getConf());
	  DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>();
	  DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
	  FSDataOutputStream fsDataOutputStream = fs.create(path);
	  dataFileWriter.create(User.SCHEMA$, fsDataOutputStream);
	  dataFileWriter.append(createUser("bill", "green"));
	  dataFileWriter.append(createUser("alice", "blue"));
	  dataFileWriter.append(createUser("cuthbert", "pink"));
	  dataFileWriter.append(createUser("belinda", "yellow"));
	  dataFileWriter.close();
	  fsDataOutputStream.close();
  }


private static User createUser(String username, String favoriteColor) {
    return User.newBuilder()
        .setUsername(username)
        .setFavoriteColor(favoriteColor)
        .setCreationDate(System.currentTimeMillis())
        .build();
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new WriteUserDataset(), args);
    System.exit(rc);
  }
}
