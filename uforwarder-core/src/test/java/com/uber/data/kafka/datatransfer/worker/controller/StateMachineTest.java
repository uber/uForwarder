package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.HeartbeatRequest;
import com.uber.data.kafka.datatransfer.HeartbeatResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.Participants;
import com.uber.data.kafka.datatransfer.RegisterWorkerRequest;
import com.uber.data.kafka.datatransfer.RegisterWorkerResponse;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class StateMachineTest extends FievelTestBase {

  private Node worker;
  private Node master;
  private ControllerClient.Factory controllerClientFactory;
  private MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub;
  private ManagedChannel channel;
  private Controllable controllable;
  private Lease lease;
  private Duration heartbeatTimeout;
  private ExecutorService commandExecutors;
  private Job job1;
  private Job job2;
  private Job job3;
  private CoreInfra infra;

  @Before
  public void setup() {
    worker = Node.newBuilder().setId(1).build();
    master = Node.newBuilder().setId(2).build();
    controllerClientFactory = Mockito.mock(ControllerClient.Factory.class);
    stub = Mockito.mock(MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub.class);
    Mockito.when(stub.withDeadlineAfter(ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
        .thenReturn(stub);
    infra = CoreInfra.NOOP;
    channel = Mockito.mock(ManagedChannel.class);
    controllable = Mockito.mock(Controllable.class);
    commandExecutors = Mockito.mock(ExecutorService.class);
    lease = Lease.forTest(1000, 0);
    heartbeatTimeout = Duration.ofSeconds(10);
    job1 = Job.newBuilder().setJobId(1).build();
    job2 = Job.newBuilder().setJobId(2).build();
    job3 = Job.newBuilder().setJobId(3).build();
  }

  /** Test that successful call to UNS will select a random master to connect to for bootstrap. */
  @Test
  public void connectingToRegistering() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();

    Mockito.when(controllerClientFactory.connect())
        .thenReturn(new ControllerClient(master, channel, stub, infra));
    State oldState =
        new StateConnecting(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable);
    // oldState is CONNECTING
    Assert.assertEquals(StateConnecting.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be REGISTERING
    Assert.assertEquals(StateRegistering.STATE, newState.toString());
    // Lease should not be extended because it is not a heartbeat success.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  /** Test that failed call to UNS will result in re-lookup to UDG. */
  @Test
  public void connectingToConnecting() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();

    Mockito.when(controllerClientFactory.connect()).thenThrow(new Exception("exception"));
    State oldState =
        new StateConnecting(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable);
    // oldState is CONNECTING
    Assert.assertEquals(StateConnecting.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Lease should not be extended because it is not a heartbeat success.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  /** Test that a successful register call to the master will move the state machine to WORKING. */
  @Test
  public void registeringToWorking() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Node workerWithNewId = Node.newBuilder(worker).setId(worker.getId() + 1).build();
    Mockito.when(
            stub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenReturn(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder().setWorker(workerWithNewId).setMaster(master).build())
                .build());

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is REGISTERING
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Lease should be reset
    Assert.assertNotEquals(lastSuccessTime, lease.lastSuccessTime());
    // Controller node should be updated to reflect the one returned by the master
    Assert.assertEquals(workerWithNewId, newState.worker);
  }

  /**
   * Test that if worker register with non-primary master, it will be redirected to the correct
   * primary master and the worker is responsible for re-issuing REGISTER request so we leave state
   * machine in REGISTERING state.
   */
  @Test
  public void registeringToRegistering() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Node workerWithNewId = Node.newBuilder(worker).setId(worker.getId() + 1).build();
    Node newMaster = Node.newBuilder(master).setId(master.getId() + 1).build();
    Mockito.when(
            stub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenReturn(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder()
                        .setWorker(workerWithNewId)
                        .setMaster(newMaster)
                        .build())
                .build());

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is REGISTERING
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be REGISTERING
    Assert.assertEquals(StateRegistering.STATE, newState.toString());
    // Lease should be reset
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  @Test
  public void registeringToConnectingCausedByEmptyWorkingInResponse() {
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                .setParticipants(Participants.newBuilder().setMaster(master).build())
                .build());
    long lastSuccessTime = lease.lastSuccessTime();

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is Registering
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // make sure the worker node does not change.
    Assert.assertEquals(oldState.worker, newState.worker);
  }

  /**
   * Test that an exception during REGISTERING results in the worker state machine transitioning
   * back to CONNECTING.
   */
  @Test
  public void registeringToConnectingCausedByRegisterFailure() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Mockito.when(
            stub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenThrow(new RuntimeException("exception"));

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is REGISTERING
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Lease should not be extended because it is not a heartbeat success.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  /**
   * Tests that an exception during REGISTERING and an exception during master client close result
   * in the worker state machine transitioning back to CONNECTING.
   */
  @Test
  public void registeringToConnectingCausedByRegisterFailureWithCloseException() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Mockito.when(
            stub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenThrow(new RuntimeException("exception"));

    Mockito.doThrow(new InterruptedException())
        .when(channel)
        .awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any());

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is REGISTERING
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Lease should not be extended because it is not a heartbeat success.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  /**
   * Test that an exception during connecting to the redirected master results in the worker state
   * machine transitioning back to CONNECTING.
   */
  @Test
  public void registeringToConnectingCausedByReconnectFailure() throws Exception {
    Mockito.when(
            controllerClientFactory.reconnectOnChange(
                ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenThrow(new IOException("exception"));

    long lastSuccessTime = lease.lastSuccessTime();
    Node workerWithNewId = Node.newBuilder(worker).setId(worker.getId() + 1).build();
    Node newMaster = Node.newBuilder(master).setId(master.getId() + 1).build();
    Mockito.when(
            stub.registerWorker(
                RegisterWorkerRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenReturn(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder()
                        .setWorker(workerWithNewId)
                        .setMaster(newMaster)
                        .build())
                .build());

    State oldState =
        new StateRegistering(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is REGISTERING
    Assert.assertEquals(StateRegistering.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Lease should be not be updated
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
  }

  /** Test worker receives and executes commands from master. */
  @Test(timeout = 10000)
  public void workingToWorking() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Mockito.when(controllable.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build()));
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(controllable.getJobStatus())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder().setWorker(worker).setMaster(master).build())
                .addCommands(
                    Command.newBuilder()
                        .setType(CommandType.COMMAND_TYPE_CANCEL_JOB)
                        .setJob(job1)
                        .build())
                .addCommands(
                    Command.newBuilder()
                        .setType(CommandType.COMMAND_TYPE_RUN_JOB)
                        .setJob(job2)
                        .build())
                .addCommands(
                    Command.newBuilder()
                        .setType(CommandType.COMMAND_TYPE_UPDATE_JOB)
                        .setJob(job3)
                        .build())
                .addCommands(Command.newBuilder().setType(CommandType.COMMAND_TYPE_INVALID).build())
                .build());
    Mockito.when(controllable.cancel(job1)).thenReturn(CompletableFuture.completedFuture(null));
    Mockito.when(controllable.run(job2)).thenReturn(CompletableFuture.completedFuture(null));
    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    completableFuture.completeExceptionally(new Exception());
    Mockito.when(controllable.update(job3)).thenReturn(completableFuture);

    Scope scope = Mockito.mock(Scope.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Counter counter = Mockito.mock(Counter.class);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Timer timer = Mockito.mock(Timer.class);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    State oldState =
        new StateWorking(
            CoreInfra.builder().withScope(scope).build(),
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Successful heartbeat so lease should be incremented
    Assert.assertTrue(lease.lastSuccessTime() > lastSuccessTime);
    // Assert that the command are issued to controllable and run the async runnables
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(commandExecutors, Mockito.times(3)).execute(runnableArgumentCaptor.capture());
    runnableArgumentCaptor.getAllValues().forEach(x -> x.run());
    // verify that each command was executed correctly
    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    Mockito.verify(controllable, Mockito.times(1)).cancel(jobArgumentCaptor.capture());
    Mockito.verify(controllable, Mockito.times(1)).run(jobArgumentCaptor.capture());
    Mockito.verify(controllable, Mockito.times(1)).update(jobArgumentCaptor.capture());
    Assert.assertEquals(job1, jobArgumentCaptor.getAllValues().get(0));
    Assert.assertEquals(job2, jobArgumentCaptor.getAllValues().get(1));
    Assert.assertEquals(job3, jobArgumentCaptor.getAllValues().get(2));
    // one heartbeat
    Mockito.verify(scope, Mockito.times(1)).counter("controller.heartbeat.success");
    // update command execution failure
    Mockito.verify(scope, Mockito.times(1)).counter(Controllable.Metrics.FAILURE);
    // one getJobStatus, one run command, one cancel command
    Mockito.verify(scope, Mockito.times(3)).counter(Controllable.Metrics.SUCCESS);
  }

  /** Tests the behavior of workers when master client is closed. */
  @Test(timeout = 10000)
  public void workingToWorkingWhenMasterClientChannelIsTerminated() throws Exception {
    Mockito.when(channel.isTerminated()).thenReturn(true);
    Mockito.when(controllerClientFactory.reconnect(ArgumentMatchers.any()))
        .thenReturn(new ControllerClient(master, channel, stub, infra));
    lease.success();
    long lastSuccessTime = lease.lastSuccessTime();

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // make sure the worker node does not change.
    Assert.assertEquals(oldState.worker, newState.worker);
    // Work should not be canceled.
    Mockito.verify(controllable, Mockito.times(0)).cancelAll();
  }

  /**
   * Test that state machines gracefully handles the case when Controllable getJobStatus and get
   * exceptions.
   *
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void workingToWorkingWithControllableFailure() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    Mockito.doThrow(new RuntimeException()).when(controllable).run(Mockito.any());
    Mockito.doThrow(new RuntimeException()).when(controllable).getJobStatus();
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(ImmutableList.of())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder().setWorker(worker).setMaster(master).build())
                .addCommands(
                    Command.newBuilder()
                        .setType(CommandType.COMMAND_TYPE_RUN_JOB)
                        .setJob(job2)
                        .build())
                .build());
    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Successful heartbeat so lease should be incremented
    Assert.assertTrue(lease.lastSuccessTime() > lastSuccessTime);
    // Assert that the command are issued to controllable and run the async runnables
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(commandExecutors, Mockito.times(1)).execute(runnableArgumentCaptor.capture());
    runnableArgumentCaptor.getAllValues().forEach(x -> x.run());
  }

  /** This tests that worker gracefully handles master leader election (without hard restart). */
  @Test(timeout = 10000)
  public void workingToWorkingFailoverOnMasterElection() throws Exception {
    lease.success();
    long lastSuccessTime = lease.lastSuccessTime();
    Node newMaster = Node.newBuilder().setId(3).build();
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(controllable.getJobStatus())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                // on the server side, it does not necessarily return the same worker node.
                .setParticipants(Participants.newBuilder().setMaster(newMaster).build())
                .build());

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // make sure the worker node does not change.
    Assert.assertEquals(oldState.worker, newState.worker);
    // make sure the reconnectOnChange method was invoked
    Mockito.verify(controllerClientFactory, Mockito.times(1))
        .reconnectOnChange(Mockito.any(), Mockito.any());
  }

  /**
   * This verifies that worker will gracefully switch master on master restart by looking up with
   * UDG and connecting to new master.
   */
  @Test(timeout = 10000)
  public void workingToWorkingFailoverOnMasterRestart() throws Exception {
    ControllerClient controllerClient = new ControllerClient(master, channel, stub, infra);
    lease.success();
    long lastSuccessTime = lease.lastSuccessTime();
    Node newMaster = Node.newBuilder().setId(3).build();
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    Mockito.when(controllerClientFactory.reconnect(ArgumentMatchers.any()))
        .thenReturn(new ControllerClient(newMaster, channel, stub, infra));

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            controllerClient);
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // Verify that master client factory is invoked for master switch
    Mockito.verify(controllerClientFactory, Mockito.times(1)).reconnect(Mockito.any());
  }

  @Test
  public void workingToConnectingCausedByEmptyWorkingInResponse() {
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                .setParticipants(Participants.newBuilder().setMaster(master).build())
                .build());
    Mockito.when(controllable.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));
    long lastSuccessTime = lease.lastSuccessTime();

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be CONNECTING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // make sure the worker node does not change.
    Assert.assertEquals(oldState.worker, newState.worker);
    // Work should be canceled.
    Mockito.verify(controllable, Mockito.times(1)).cancelAll();
  }

  /**
   * Test that worker transitions to CONNECTING and cancelAll jobs if lease expires even if canel
   * threw exception.
   */
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 10000)
  public void workingToConnectingWithControllableException() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new RuntimeException("error"));
    CompletableFuture cancelFuture = new CompletableFuture();
    cancelFuture.completeExceptionally(new RuntimeException());
    Mockito.when(controllable.cancelAll()).thenReturn(cancelFuture);

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Wait for the lease to expire.
    Thread.sleep(1000 * 3);
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // Work should be canceled.
    Mockito.verify(controllable, Mockito.times(1)).cancelAll();
  }

  /**
   * Test that worker transitions to CONNECTING and cancelAll jobs if heart beat fails and lease
   * expires.
   */
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 10000)
  public void workingToConnectingCausedByHeartbeatFailureWhenLeaseExpires() throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new RuntimeException("error"));
    Mockito.when(controllable.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // Work should be canceled.
    Mockito.verify(controllable, Mockito.times(1)).cancelAll();
  }

  /**
   * Test that worker still connecting in the following situations: 1. old leader not available 2.
   * new leader available after reconnect 3. lease not expire
   */
  @Test
  public void workingToWorkingCausedByHeartbeatFailureWhenLeaseNotExpires() throws Exception {
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    Mockito.when(controllerClientFactory.reconnect(ArgumentMatchers.any()))
        .thenReturn(new ControllerClient(master, channel, stub, infra));

    lease.success();
    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
  }

  /**
   * Test that worker transitions to CONNECTING and cancelAll jobs if heart beat fails, lease
   * expires and master client close fails.
   */
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 10000)
  public void workingToConnectingCausedByHeartbeatFailureWhenLeaseExpiresAndCloseException()
      throws Exception {
    long lastSuccessTime = lease.lastSuccessTime();
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new RuntimeException("error"));
    Mockito.when(controllable.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));

    Mockito.doThrow(new InterruptedException())
        .when(channel)
        .awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any());

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Wait for the lease to expire.
    Thread.sleep(1000 * 3);
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateConnecting.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // Work should be canceled.
    Mockito.verify(controllable, Mockito.times(1)).cancelAll();
  }

  /**
   * This tests that worker transitions to CONNECTING and cancelAll jobs when reconnecting to a new
   * master failed
   */
  @Test(timeout = 10000)
  public void workingToWorkingCausedByReconnectFailureOnMasterElection() throws Exception {
    Mockito.when(
            controllerClientFactory.reconnectOnChange(
                ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenThrow(new IOException("exception"));
    Mockito.when(controllerClientFactory.reconnect(ArgumentMatchers.any()))
        .thenReturn(new ControllerClient(master, channel, stub, infra));
    lease.success();
    long lastSuccessTime = lease.lastSuccessTime();
    Node newMaster = Node.newBuilder().setId(3).build();
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(controllable.getJobStatus())
                    .build()))
        .thenReturn(
            HeartbeatResponse.newBuilder()
                // on the server side, it does not necessarily return the same worker node.
                .setParticipants(Participants.newBuilder().setMaster(newMaster).build())
                .build());
    Mockito.when(controllable.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // make sure the worker node does not change.
    Assert.assertEquals(oldState.worker, newState.worker);
    // Work should not be canceled.
    Mockito.verify(controllable, Mockito.times(0)).cancelAll();
  }

  /**
   * This tests that worker transitions to CONNECTING and cancelAll jobs when reconnecting to a new
   * master failed after lease expires.
   */
  @Test(timeout = 10000)
  public void workingToWorkingCausedByReconnectFailureOnMasterRestart() throws Exception {
    lease.success();
    long lastSuccessTime = lease.lastSuccessTime();
    List<JobStatus> jobStatusList =
        ImmutableList.of(
            JobStatus.newBuilder().setJob(job1).setState(JobState.JOB_STATE_RUNNING).build());
    Mockito.when(controllable.getJobStatus()).thenReturn(jobStatusList);
    Mockito.when(
            stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setParticipants(
                        Participants.newBuilder().setWorker(worker).setMaster(master).build())
                    .addAllJobStatus(jobStatusList)
                    .build()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE));
    Mockito.when(controllerClientFactory.reconnect(ArgumentMatchers.any()))
        .thenThrow(new IOException());
    Mockito.when(controllable.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));

    State oldState =
        new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra));
    // oldState is WORKING
    Assert.assertEquals(StateWorking.STATE, oldState.toString());
    // Transition state.
    State newState = oldState.nextState();
    // newState should be WORKING
    Assert.assertEquals(StateWorking.STATE, newState.toString());
    // Heartbeat failed so lease should not be updated.
    Assert.assertEquals(lastSuccessTime, lease.lastSuccessTime());
    // Verify that master client factory is invoked for master switch
    Mockito.verify(controllerClientFactory, Mockito.times(1)).reconnect(Mockito.any());
    // Work should not be canceled.
    Mockito.verify(controllable, Mockito.times(0)).cancelAll();
  }

  @Test
  public void testMarkWarn() {
    new StateWorking(
            infra,
            worker,
            controllerClientFactory,
            lease,
            heartbeatTimeout,
            commandExecutors,
            controllable,
            new ControllerClient(master, channel, stub, infra))
        .markWarn(
            "message",
            "working",
            "working",
            com.uber.m3.util.Duration.ZERO,
            "master",
            new RuntimeException());
  }
}
