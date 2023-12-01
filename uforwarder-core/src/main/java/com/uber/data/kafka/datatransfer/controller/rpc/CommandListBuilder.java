package com.uber.data.kafka.datatransfer.controller.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.m3.tally.Scope;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CommandListBuilder builds the command list for this worker via the Command Pattern
 * (https://en.wikipedia.org/wiki/Command_pattern).
 *
 * <p>Similar to many systems (e.g., Odin) the Data Transfer Master stores the expected and actual
 * state for each job. Unlike these other systems, the reconciler logic is run by the master and
 * distributed to the workers via Commands. This method is responsible for running the reconciler
 * loop and building the appropriate commands to transition the worker towards the expected state.
 * We prefer running the reconciler logic within the master b/c that allows us to keep the worker
 * thinner.
 *
 * <p>The command matrix for every actual and expected state pair is:
 *
 * <pre>
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | -             | Expected State | null           | INVALID        | UNIMPLEMENTED  | FAILED         | CANCELED       | RUNNING                                |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | Actual State  | -              | -              | -              | -              | -              | -              | -                                      |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | null          | -              | N/A            | N/A            | N/A            | N/A            | N/A            | COMMAND_RUN                            |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | INVALID       | -              | N/A            | N/A            | N/A            | N/A            | N/A            | COMMAND_RUN                            |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | UNIMPLEMENTED | -              | N/A            | N/A            | N/A            | N/A            | N/A            | COMMAND_RUN                            |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | FAILED        | -              | N/A            | N/A            | N/A            | N/A            | N/A            | COMMAND_RUN                            |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | CANCELED      | -              | N/A            | N/A            | N/A            | N/A            | N/A            | COMMAND_RUN                            |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 *  | RUNNING       | -              | COMMAND_CANCEL | COMMAND_CANCEL | COMMAND_CANCEL | COMMAND_CANCEL | COMMAND_CANCEL | COMMAND_UPDATE                         |
 *  |               |                |                |                |                |                |                | if configuration change                |
 *  +---------------+----------------+----------------+----------------+----------------+----------------+----------------+----------------------------------------+
 * </pre>
 */
public class CommandListBuilder {
  private final Scope scope;
  private final Map<Long, Command> commandMap;

  CommandListBuilder(Scope scope) {
    this.scope = scope;
    this.commandMap = new HashMap<>();
  }

  public CommandListBuilder add(long jobId, StoredJob expected, JobStatus actual) {
    switch (expected.getState()) {
      case JOB_STATE_RUNNING:
        // expected == RUNNING, actual != RUNNING, so issue RUN command.
        // Note: in the current implementation, RUN command will also be issued for UNIMPLEMENTED,
        // CANCELED and FAILED actual states.
        if (actual.getState() != JobState.JOB_STATE_RUNNING) {
          commandMap.put(jobId, buildCommand(expected.getJob(), CommandType.COMMAND_TYPE_RUN_JOB));
        } else if (!JobUtils.isSameExceptStartOffset(expected.getJob(), actual.getJob())) {
          // expected == RUNNING, actual == RUNNING, but config change, so issue UPDATE command.
          // The reason for excluding start offset:
          // 1. Expected state start offset will be incremented as progress is made.
          // 2. A running job with a different start offset should not be updated.
          // 3. If a job is rebalanced, it should start from a the updated start offset
          commandMap.put(
              jobId, buildCommand(expected.getJob(), CommandType.COMMAND_TYPE_UPDATE_JOB));
        }
        break;
      default:
        // All other expected states map to CANCEL action.
        // If expected != RUNNING and actual == RUNNING, then issue CANCEL.
        // Else, we assume it is not running and we don't have to issue CANCEL.
        if (actual.getState() == JobState.JOB_STATE_RUNNING) {
          // As the expected job might be empty, we should use the actual job to issue cancel
          // command
          commandMap.put(jobId, buildCommand(actual.getJob(), CommandType.COMMAND_TYPE_CANCEL_JOB));
          break;
        }
    }
    return this;
  }

  private Command buildCommand(Job job, CommandType command) {
    scope
        .tagged(ImmutableMap.of(StructuredLogging.COMMAND_TYPE, command.toString()))
        .counter("masterworkerservice.workers.command.issued")
        .inc(1);
    return Command.newBuilder().setType(command).setJob(job).build();
  }

  public List<Command> build() {
    return ImmutableList.copyOf(commandMap.values());
  }
}
