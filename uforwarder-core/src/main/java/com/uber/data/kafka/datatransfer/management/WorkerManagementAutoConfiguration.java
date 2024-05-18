package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManagerAutoConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties({ManagementServerConfiguration.class})
@Import({PipelineManagerAutoConfiguration.class})
@Profile("data-transfer-worker")
public class WorkerManagementAutoConfiguration {
  @Bean
  public JobsHtml jobsHtml() throws Exception {
    return new JobsHtml("workerJobs.html");
  }

  @Bean
  public WorkerJobsJson jobsJson(
      PipelineManager pipelineManager,
      Node node,
      ManagementServerConfiguration managementServerConfiguration) {
    return new WorkerJobsJson(
        pipelineManager, node.getHost(), managementServerConfiguration.getDebugUrlFormat());
  }

  @Bean
  public JobStatusJson jobStatusJson(PipelineManager pipelineManager) {
    return new JobStatusJson(pipelineManager);
  }

  @Bean
  public JobStatusHtml jobStatusHtml() throws Exception {
    return new JobStatusHtml();
  }

  @Bean
  public NavJson navJson(Node node, @Value("${service.name}") String serviceName) {
    return new NavJson(serviceName, node.getHost(), "worker");
  }
}
