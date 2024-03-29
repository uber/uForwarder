package com.uber.data.kafka.datatransfer.controller.rpc;

import com.google.api.core.InternalApi;
import com.google.rpc.PreconditionFailure;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.AddJobGroupResponse;
import com.uber.data.kafka.datatransfer.DeleteJobGroupRequest;
import com.uber.data.kafka.datatransfer.DeleteJobGroupResponse;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsRequest;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsResponse;
import com.uber.data.kafka.datatransfer.GetJobGroupRequest;
import com.uber.data.kafka.datatransfer.GetJobGroupResponse;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.MasterAdminServiceGrpc;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.UpdateJobGroupRequest;
import com.uber.data.kafka.datatransfer.UpdateJobGroupResponse;
import com.uber.data.kafka.datatransfer.UpdateJobGroupStateRequest;
import com.uber.data.kafka.datatransfer.UpdateJobGroupStateResponse;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.instrumentation.BiConsumerConverter;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.data.kafka.instrumentation.ThrowingBiConsumer;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InternalApi
public final class ControllerAdminService
    extends MasterAdminServiceGrpc.MasterAdminServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(ControllerAdminService.class);
  private final CoreInfra infra;
  private final Store<String, StoredJobGroup> jobGroupStore;
  private final Store<Long, StoredJobStatus> jobStatusStore;
  private final LeaderSelector leaderSelector;

  public ControllerAdminService(
      CoreInfra infra,
      Store<String, StoredJobGroup> jobGroupStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      LeaderSelector leaderSelector) {
    this.infra = infra;
    this.jobGroupStore = jobGroupStore;
    this.jobStatusStore = jobStatusStore;
    this.leaderSelector = leaderSelector;
  }

  private <Req, Res, E extends Exception> BiConsumer<Req, StreamObserver<Res>> withLeaderRedirect(
      ThrowingBiConsumer<Req, StreamObserver<Res>, E> handler) {
    return (req, resW) -> {
      if (!leaderSelector.isLeader()) {
        String leaderId = leaderSelector.getLeaderId();

        // gRPC Metadata for storing redirect details
        Metadata redirectDetails = new Metadata();
        redirectDetails.put(
            ProtoUtils.keyForProto(PreconditionFailure.getDefaultInstance()),
            PreconditionFailure.newBuilder()
                .addViolations(
                    PreconditionFailure.Violation.newBuilder()
                        .setType("not-leader")
                        .setSubject("data-transfer")
                        .setDescription(leaderId)
                        .build())
                .build());

        // Short description which is useful for curl but should not be read by code.
        String shortMessage = leaderId;

        // Send back redirect as gRPC error
        resW.onError(
            Status.FAILED_PRECONDITION
                .withDescription(shortMessage)
                .asRuntimeException(redirectDetails));
      } else {
        BiConsumerConverter.uncheck(handler).accept(req, resW);
      }
    };
  }

  @Override
  public void addJobGroup(
      AddJobGroupRequest request, StreamObserver<AddJobGroupResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              JobGroup jobGroup = req.getJobGroup();
              Versioned<StoredJobGroup> storedJobGroup;
              try {
                storedJobGroup = jobGroupStore.get(jobGroup.getJobGroupId());
                resW.onNext(
                    AddJobGroupResponse.newBuilder().setGroup(storedJobGroup.model()).build());
              } catch (NoSuchElementException e) {
                logger.debug(
                    "JobGroup doesn't exist, will create.",
                    StructuredLogging.jobGroupId(jobGroup.getJobGroupId()));
                storedJobGroup =
                    jobGroupStore.create(
                        StoredJobGroup.newBuilder().setJobGroup(jobGroup).build(),
                        JobUtils::withJobGroupId);

                StoredJobGroup runningJobGroup =
                    StoredJobGroup.newBuilder(storedJobGroup.model())
                        .setState(req.getJobGroupState())
                        .build();
                jobGroupStore.put(
                    storedJobGroup.model().getJobGroup().getJobGroupId(),
                    VersionedProto.from(runningJobGroup, storedJobGroup.version()));
                resW.onNext(AddJobGroupResponse.newBuilder().setGroup(runningJobGroup).build());
              }

              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.addjobgroup");
  }

  @Override
  public void updateJobGroup(
      UpdateJobGroupRequest request, StreamObserver<UpdateJobGroupResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              String id = req.getJobGroup().getJobGroupId();
              Versioned<StoredJobGroup> oldJobGroup = jobGroupStore.get(id);
              StoredJobGroup newJobGroup =
                  StoredJobGroup.newBuilder(oldJobGroup.model())
                      .setJobGroup(req.getJobGroup())
                      .build();
              jobGroupStore.put(
                  newJobGroup.getJobGroup().getJobGroupId(),
                  VersionedProto.from(newJobGroup, oldJobGroup.version()));
              resW.onNext(UpdateJobGroupResponse.newBuilder().setGroup(newJobGroup).build());
              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.updatejobgroup");
  }

  @Override
  public void updateJobGroupState(
      UpdateJobGroupStateRequest request,
      StreamObserver<UpdateJobGroupStateResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              String id = req.getId();
              JobState jobState = req.getState();
              Versioned<StoredJobGroup> oldJobGroup = jobGroupStore.get(id);
              StoredJobGroup newJobGroup =
                  StoredJobGroup.newBuilder(oldJobGroup.model()).setState(jobState).build();
              jobGroupStore.put(
                  newJobGroup.getJobGroup().getJobGroupId(),
                  VersionedProto.from(newJobGroup, oldJobGroup.version()));
              resW.onNext(UpdateJobGroupStateResponse.newBuilder().setGroup(newJobGroup).build());
              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.updatejobgroupstate");
  }

  @Override
  public void deleteJobGroup(
      DeleteJobGroupRequest request, StreamObserver<DeleteJobGroupResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              String id = req.getId();
              jobGroupStore.remove(id);
              resW.onNext(DeleteJobGroupResponse.newBuilder().build());
              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.deletejobgroup");
  }

  @Override
  public void getJobGroup(
      GetJobGroupRequest request, StreamObserver<GetJobGroupResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              String id = req.getId();
              Versioned<StoredJobGroup> jobGroup = jobGroupStore.get(id);
              resW.onNext(GetJobGroupResponse.newBuilder().setGroup(jobGroup.model()).build());
              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.getjobgroup");
  }

  @Override
  public void getAllJobGroups(
      GetAllJobGroupsRequest request, StreamObserver<GetAllJobGroupsResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              Map<String, Versioned<StoredJobGroup>> jobGroupMap = jobGroupStore.getAll();
              for (Versioned<StoredJobGroup> versionedJobGroup : jobGroupMap.values()) {
                resW.onNext(
                    GetAllJobGroupsResponse.newBuilder()
                        .setJobGroup(versionedJobGroup.model().getJobGroup())
                        .setJobGroupState(versionedJobGroup.model().getState())
                        .build());
              }
              resW.onCompleted();
            }),
        request,
        responseObserver,
        "masteradminservice.getalljobgroups");
  }
}
