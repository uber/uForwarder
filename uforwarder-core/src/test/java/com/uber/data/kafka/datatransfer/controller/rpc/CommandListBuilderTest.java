package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.m3.tally.NoopScope;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CommandListBuilderTest {
  public JobStatus actualJob;
  public StoredJob expectedJob;
  @Nullable public CommandType command;

  /** Use JUnit parameterized tests to test every combination of actual and expected states. */
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // Parameters follow format:
          // actual state, expected state, command to issue (null == no COMMAND issued)
          // if there is no reported job status, protobuf will default to JOB_STATE_INVALID so
          // actual(INVALID) tests the case of empty job status from worker.
          // similarly, if there is no expected job for a job status that is reported from worker,
          // protobuf will default to INVALID so expected(INVALID) tests the case where there is no
          // job in ZK for a corresponding job status from the worker.
          {actual(JobState.JOB_STATE_INVALID), expected(JobState.JOB_STATE_INVALID), null},
          {actual(JobState.JOB_STATE_INVALID), expected(JobState.JOB_STATE_CANCELED), null},
          {actual(JobState.JOB_STATE_INVALID), expected(JobState.JOB_STATE_FAILED), null},
          {actual(JobState.JOB_STATE_INVALID), expected(JobState.JOB_STATE_UNIMPLEMENTED), null},
          {
            actual(JobState.JOB_STATE_INVALID),
            expected(JobState.JOB_STATE_RUNNING, newJob()),
            CommandType.COMMAND_TYPE_RUN_JOB
          },
          {actual(JobState.JOB_STATE_UNIMPLEMENTED), expected(JobState.JOB_STATE_INVALID), null},
          {actual(JobState.JOB_STATE_UNIMPLEMENTED), expected(JobState.JOB_STATE_CANCELED), null},
          {actual(JobState.JOB_STATE_UNIMPLEMENTED), expected(JobState.JOB_STATE_FAILED), null},
          {
            actual(JobState.JOB_STATE_UNIMPLEMENTED),
            expected(JobState.JOB_STATE_UNIMPLEMENTED),
            null
          },
          {
            actual(JobState.JOB_STATE_UNIMPLEMENTED),
            expected(JobState.JOB_STATE_RUNNING, newJob()),
            CommandType.COMMAND_TYPE_RUN_JOB
          },
          {actual(JobState.JOB_STATE_FAILED), expected(JobState.JOB_STATE_INVALID), null},
          {actual(JobState.JOB_STATE_FAILED), expected(JobState.JOB_STATE_CANCELED), null},
          {actual(JobState.JOB_STATE_FAILED), expected(JobState.JOB_STATE_FAILED), null},
          {actual(JobState.JOB_STATE_FAILED), expected(JobState.JOB_STATE_UNIMPLEMENTED), null},
          {
            actual(JobState.JOB_STATE_FAILED),
            expected(JobState.JOB_STATE_RUNNING, newJob()),
            CommandType.COMMAND_TYPE_RUN_JOB
          },
          {actual(JobState.JOB_STATE_CANCELED), expected(JobState.JOB_STATE_INVALID), null},
          {actual(JobState.JOB_STATE_CANCELED), expected(JobState.JOB_STATE_CANCELED), null},
          {actual(JobState.JOB_STATE_CANCELED), expected(JobState.JOB_STATE_FAILED), null},
          {actual(JobState.JOB_STATE_CANCELED), expected(JobState.JOB_STATE_UNIMPLEMENTED), null},
          {
            actual(JobState.JOB_STATE_CANCELED),
            expected(JobState.JOB_STATE_RUNNING, newJob()),
            CommandType.COMMAND_TYPE_RUN_JOB
          },
          {
            actual(JobState.JOB_STATE_RUNNING, newJob()),
            expected(JobState.JOB_STATE_INVALID),
            CommandType.COMMAND_TYPE_CANCEL_JOB
          },
          {
            actual(JobState.JOB_STATE_RUNNING, newJob()),
            expected(JobState.JOB_STATE_CANCELED),
            CommandType.COMMAND_TYPE_CANCEL_JOB
          },
          {
            actual(JobState.JOB_STATE_RUNNING, newJob()),
            expected(JobState.JOB_STATE_FAILED),
            CommandType.COMMAND_TYPE_CANCEL_JOB
          },
          {
            actual(JobState.JOB_STATE_RUNNING, newJob()),
            expected(JobState.JOB_STATE_UNIMPLEMENTED),
            CommandType.COMMAND_TYPE_CANCEL_JOB
          },
          {actual(JobState.JOB_STATE_RUNNING), expected(JobState.JOB_STATE_RUNNING), null},
          // if actual == expected == RUNNING but configurations are different, send the UPDATE
          // command
          {
            actual(JobState.JOB_STATE_RUNNING),
            expected(
                JobState.JOB_STATE_RUNNING,
                Job.newBuilder()
                    .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(100).build())
                    .build()),
            CommandType.COMMAND_TYPE_UPDATE_JOB
          },
        });
  }

  @MethodSource("data")
  @ParameterizedTest
  public void test(JobStatus actualJob, StoredJob expectedJob, CommandType command) {
    initCommandListBuilderTest(actualJob, expectedJob, command);
    List<Command> commandList =
        new CommandListBuilder(new NoopScope()).add(1L, expectedJob, actualJob).build();
    Assertions.assertEquals(command, getCommandType(commandList));
    if (command != null) {
      switch (command) {
        case COMMAND_TYPE_CANCEL_JOB:
          Assertions.assertEquals(actualJob.getJob(), commandList.get(0).getJob());
          break;
        case COMMAND_TYPE_RUN_JOB:
        case COMMAND_TYPE_UPDATE_JOB:
          Assertions.assertEquals(expectedJob.getJob(), commandList.get(0).getJob());
          break;
      }
    }
  }

  @Nullable
  private static CommandType getCommandType(@Nullable List<Command> commandList) {
    return commandList.size() == 0 ? null : commandList.get(0).getType();
  }

  private static StoredJob expected(JobState state) {
    return expected(state, Job.newBuilder().build());
  }

  private static StoredJob expected(JobState state, Job job) {
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.setState(state);
    builder.setJob(job);
    return builder.build();
  }

  private static JobStatus actual(JobState state) {
    return actual(state, Job.newBuilder().build());
  }

  private static JobStatus actual(JobState state, Job job) {
    JobStatus.Builder builder = JobStatus.newBuilder();
    builder.setState(state);
    builder.setJob(job);
    return builder.build();
  }

  private static Job newJob() {
    return Job.newBuilder()
        .setJobId(1)
        .setKafkaConsumerTask(
            KafkaConsumerTask.newBuilder()
                .setTopic("topic")
                .setConsumerGroup("group")
                .setCluster("cluster")
                .build())
        .build();
  }

  public void initCommandListBuilderTest(
      JobStatus actualJob, StoredJob expectedJob, CommandType command) {
    this.actualJob = actualJob;
    this.expectedJob = expectedJob;
    this.command = command;
  }
}
