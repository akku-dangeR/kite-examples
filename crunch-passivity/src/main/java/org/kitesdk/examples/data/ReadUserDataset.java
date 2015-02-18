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

import java.net.URI;
import java.util.Collection;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.crunch.CrunchDatasets;

/**
 * Read the user objects from the users dataset by key lookup, and by scanning.
 */
public class ReadUserDataset extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
	  
	  URI datasetUri = URI.create("dataset:hdfs:test_crunch_passivity/users");
	  Dataset<User> userDataset = null;
	  if (!Datasets.exists(datasetUri)) {
		  throw new RuntimeException("Dataset does not exist: " + datasetUri.toString());
	  } else {
		  userDataset = Datasets.load(datasetUri, User.class);
	  }
	  
	  Pipeline pipeline = new MRPipeline(getClass(), getConf());
	  PCollection<User> users = pipeline.read(CrunchDatasets.asSource(userDataset));
	  users.write(To.avroFile(new Path("/tmp/users/" + System.currentTimeMillis())));
	  Collection<User> usersAsCollection = users.asCollection().getValue();
	  System.out.println("Printing users");
	  System.out.println("Schema: " + User.SCHEMA$.toString(true));
	  System.out.println("----------------------------------------");
	  for (User user : usersAsCollection) {
		  System.out.println(user);
  	  }
	  PipelineResult result = pipeline.done();
	  
	  if (result != null && PipelineExecution.Status.SUCCEEDED == result.status.SUCCEEDED) {
		  return 0;
	  }
	  
	  throw new RuntimeException("Failure reading from dataset with uri: " + datasetUri.toString());
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new ReadUserDataset(), args);
    System.exit(rc);
  }
}
