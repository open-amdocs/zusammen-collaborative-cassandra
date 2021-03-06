/*
 * Copyright © 2016-2017 European Support Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amdocs.zusammen.plugin.main;

import com.amdocs.zusammen.commons.health.data.HealthInfo;
import com.amdocs.zusammen.commons.health.data.HealthStatus;
import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.Namespace;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.Space;
import com.amdocs.zusammen.datatypes.item.Action;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.datatypes.item.Info;
import com.amdocs.zusammen.datatypes.item.ItemVersion;
import com.amdocs.zusammen.datatypes.item.ItemVersionData;
import com.amdocs.zusammen.datatypes.item.ItemVersionDataConflict;
import com.amdocs.zusammen.datatypes.item.ItemVersionStatus;
import com.amdocs.zusammen.datatypes.item.Resolution;
import com.amdocs.zusammen.datatypes.itemversion.ItemVersionRevisions;
import com.amdocs.zusammen.datatypes.itemversion.Revision;
import com.amdocs.zusammen.datatypes.itemversion.Tag;
import com.amdocs.zusammen.datatypes.response.ErrorCode;
import com.amdocs.zusammen.datatypes.response.Module;
import com.amdocs.zusammen.datatypes.response.Response;
import com.amdocs.zusammen.datatypes.response.ReturnCode;
import com.amdocs.zusammen.datatypes.response.ZusammenException;
import com.amdocs.zusammen.plugin.ZusammenPluginUtil;
import com.amdocs.zusammen.plugin.collaboration.CommitStagingService;
import com.amdocs.zusammen.plugin.collaboration.DiscardChangesService;
import com.amdocs.zusammen.plugin.collaboration.ElementPrivateStore;
import com.amdocs.zusammen.plugin.collaboration.ElementPublicStore;
import com.amdocs.zusammen.plugin.collaboration.ElementStageStore;
import com.amdocs.zusammen.plugin.collaboration.PublishService;
import com.amdocs.zusammen.plugin.collaboration.RevertService;
import com.amdocs.zusammen.plugin.collaboration.SyncService;
import com.amdocs.zusammen.plugin.collaboration.VersionPrivateStore;
import com.amdocs.zusammen.plugin.collaboration.VersionPublicStore;
import com.amdocs.zusammen.plugin.collaboration.VersionStageStore;
import com.amdocs.zusammen.plugin.collaboration.impl.ElementPrivateStoreImpl;
import com.amdocs.zusammen.plugin.collaboration.impl.ElementPublicStoreImpl;
import com.amdocs.zusammen.plugin.collaboration.impl.ElementStageStoreImpl;
import com.amdocs.zusammen.plugin.collaboration.impl.VersionPrivateStoreImpl;
import com.amdocs.zusammen.plugin.collaboration.impl.VersionPublicStoreImpl;
import com.amdocs.zusammen.plugin.collaboration.impl.VersionStageStoreImpl;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;
import com.amdocs.zusammen.plugin.dao.types.StageEntity;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.dao.types.VersionDataElement;
import com.amdocs.zusammen.plugin.dao.types.VersionEntity;
import com.amdocs.zusammen.sdk.collaboration.CollaborationStore;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationElement;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationElementConflict;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationItemVersionConflict;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationMergeChange;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationMergeResult;
import com.amdocs.zusammen.sdk.collaboration.types.CollaborationPublishResult;
import com.amdocs.zusammen.sdk.types.ElementConflictDescriptor;
import com.amdocs.zusammen.sdk.types.ElementDescriptor;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amdocs.zusammen.datatypes.item.SynchronizationStatus.MERGING;
import static com.amdocs.zusammen.datatypes.item.SynchronizationStatus.OUT_OF_SYNC;
import static com.amdocs.zusammen.datatypes.item.SynchronizationStatus.UP_TO_DATE;
import static com.amdocs.zusammen.plugin.ZusammenPluginConstants.ROOT_ELEMENTS_PARENT_ID;

public class CassandraCollaborationStorePluginImpl implements CollaborationStore {
  // TODO: 8/15/2017 inject
  private VersionPrivateStore versionPrivateStore = new VersionPrivateStoreImpl();
  private VersionPublicStore versionPublicStore = new VersionPublicStoreImpl();
  private VersionStageStore versionStageStore = new VersionStageStoreImpl();

  private ElementPrivateStore elementPrivateStore = new ElementPrivateStoreImpl();
  private ElementPublicStore elementPublicStore = new ElementPublicStoreImpl();
  private ElementStageStore elementStageStore = new ElementStageStoreImpl();

  // TODO: 9/4/2017
  private PublishService publishService =
      new PublishService(versionPublicStore, versionPrivateStore, elementPublicStore,
          elementPrivateStore);
  private DiscardChangesService discardChangesService =
      new DiscardChangesService(versionPublicStore, versionPrivateStore, elementPublicStore,
          elementPrivateStore, elementStageStore);
  private SyncService syncService =
      new SyncService(versionPublicStore, versionPrivateStore, versionStageStore,
          elementPublicStore, elementPrivateStore, elementStageStore);
  private CommitStagingService commitStagingService =
      new CommitStagingService(versionPrivateStore, versionStageStore, elementPrivateStore,
          elementStageStore);
  private RevertService revertService =
      new RevertService(elementPublicStore, elementPrivateStore);

  @Override
  public Response<Void> createItem(SessionContext context, Id itemId, Info info) {
    // done by state store
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> deleteItem(SessionContext context, Id itemId) {
    versionPrivateStore.list(context, itemId)
        .forEach(version -> deleteItemVersion(context, itemId, version.getId()));

    versionPublicStore.list(context, itemId)
        .forEach(version -> {
          elementPublicStore.cleanAll(context, new ElementContext(itemId, version.getId()));
          versionPublicStore.delete(context, itemId, version);
        });

    // delete item done by state store
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> createItemVersion(SessionContext context, Id itemId, Id baseVersionId,
                                          Id versionId, ItemVersionData itemVersionData) {
    Date creationTime = new Date();
    versionPrivateStore.create(context, itemId,
        ZusammenPluginUtil
            .convertToVersionEntity(versionId, baseVersionId, creationTime, creationTime));

    ElementContext elementContext = new ElementContext(itemId, versionId);
    VersionDataElement versionData = new VersionDataElement(itemVersionData);

    if (baseVersionId == null) {
      elementPrivateStore.create(context, elementContext, versionData);
    } else {
      copyElements(context, new ElementContext(itemId, baseVersionId), elementContext);
      elementPrivateStore.update(context, elementContext, versionData);
    }

    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> updateItemVersion(SessionContext context, Id itemId, Id versionId,
                                          ItemVersionData itemVersionData) {

    if (elementPrivateStore.update(context, new ElementContext(itemId, versionId),
        new VersionDataElement(itemVersionData))) {

      VersionEntity version = new VersionEntity(versionId);
      version.setModificationTime(new Date());
      versionPrivateStore.update(context, itemId, version);
    }

    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> deleteItemVersion(SessionContext context, Id itemId, Id versionId) {
    ElementContext elementContext = new ElementContext(itemId, versionId);
    VersionEntity version = new VersionEntity(versionId);

    elementStageStore.deleteAll(context, elementContext);
    versionStageStore.delete(context, itemId, version);

    elementPrivateStore.cleanAll(context, elementContext);
    versionPrivateStore.delete(context, itemId, version);
    return new Response(Void.TYPE);
  }

  @Override
  public Response<ItemVersionStatus> getItemVersionStatus(SessionContext context, Id itemId,
                                                          Id versionId) {
    if (versionStageStore.get(context, itemId, new VersionEntity(versionId)).isPresent()) {
      return new Response<>(new ItemVersionStatus(MERGING, true));
    }

    Optional<SynchronizationStateEntity> publicSyncState =
        versionPublicStore.getSynchronizationState(context, itemId, versionId, null);

    if (!publicSyncState.isPresent()) {
      return new Response<>(new ItemVersionStatus(UP_TO_DATE, true));
    }

    SynchronizationStateEntity privateSyncState =
        versionPrivateStore.getSynchronizationState(context, itemId, versionId)
            // TODO: 7/18/2017 ?
            .orElseThrow(() -> new IllegalStateException("private version must exist"));

    return new Response<>(new ItemVersionStatus(
        privateSyncState.getPublishTime().equals(publicSyncState.get().getPublishTime())
            ? UP_TO_DATE
            : OUT_OF_SYNC,
        privateSyncState.isDirty()));
  }

  @Override
  public Response<Void> tagItemVersion(SessionContext context, Id itemId, Id versionId,
                                       Id revisionId,
                                       Tag tag) {
   /* if (revisionId != null) {
      throw new UnsupportedOperationException(
          "In this plugin implementation tag is supported only on versionId");
    }

    copyElements(context,
        new ElementContext(itemId, versionId),
        new ElementContext(itemId, versionId, tag.getName()));*/

    return new Response(Void.TYPE);
  }

  @Override
  public Response<CollaborationPublishResult> publishItemVersion(SessionContext context,
                                                                 Id itemId, Id versionId,
                                                                 String message) {
    try {
      return new Response<>(publishService.publish(context, itemId, versionId, message));
    } catch (ZusammenException ze) {
      return new Response<>(
          new ReturnCode(ErrorCode.CL_ITEM_VERSION_PUBLISH, Module.ZCSP, null, ze.getReturnCode()));
    }
  }

  @Override
  public Response<CollaborationMergeResult> syncItemVersion(SessionContext context, Id itemId,
                                                            Id versionId) {
    CollaborationMergeResult result = syncService.sync(context, itemId, versionId);
    commitStagingService.commitStaging(context, itemId, versionId);

    return new Response<>(result);
  }

  @Override
  public Response<CollaborationMergeResult> forceSyncItemVersion(SessionContext context, Id itemId,
                                                                 Id versionId) {
    discardItemVersionChanges(context, itemId, versionId);
    return syncItemVersion(context, itemId, versionId);
  }

  @Override
  public Response<CollaborationMergeResult> mergeItemVersion(SessionContext context, Id itemId,
                                                             Id versionId, Id sourceVersionId) {
    throw new UnsupportedOperationException("mergeItemVersion");
  }

  @Override
  public Response<CollaborationItemVersionConflict> getItemVersionConflict(SessionContext context,
                                                                           Id itemId,
                                                                           Id versionId) {
    ElementContext elementContext = new ElementContext(itemId, versionId, Id.ZERO);

    Collection<StageEntity<ElementEntity>> conflictedStagedElementDescriptors =
        elementStageStore.listConflictedDescriptors(context, elementContext);

    CollaborationItemVersionConflict result = new CollaborationItemVersionConflict();
    for (StageEntity<ElementEntity> stagedElementDescriptor : conflictedStagedElementDescriptors) {
      if (ROOT_ELEMENTS_PARENT_ID.equals(stagedElementDescriptor.getEntity().getId())) {
        result.setVersionDataConflict(
            getVersionDataConflict(context, elementContext, stagedElementDescriptor));
      } else {
        result.getElementConflictDescriptors()
            .add(getElementConflictDescriptor(context, elementContext, stagedElementDescriptor));
      }
    }
    return new Response<>(result);
  }

  @Override
  public Response<ItemVersionRevisions> listItemVersionRevisions(SessionContext context, Id itemId,
                                                                 Id versionId) {
    List<SynchronizationStateEntity> syncStates =
        versionPublicStore.listSynchronizationStates(context, itemId, versionId);

    ItemVersionRevisions itemVersionRevisions = new ItemVersionRevisions();
    syncStates.forEach(
        syncState -> itemVersionRevisions.addChange(convertSyncStateToRevision(syncState)));

    return new Response<>(itemVersionRevisions);
  }

  @Override
  public Response<Revision> getItemVersionRevision(SessionContext context, Id itemId, Id versionId,
                                                   Id revisionId) {
    throw new UnsupportedOperationException(
        "get revision is not supported in the current cassandra plugin");
  }

  @Override
  public Response<CollaborationMergeChange> resetItemVersionRevision(SessionContext context,
                                                                     Id itemId, Id versionId,
                                                                     Id revisionId) {
    throw new UnsupportedOperationException("resetItemVersionRevision function not supported");

  }

  /**
   * Changes private content to be equal to the specified revision.
   * The changes required in order to do so are marked as dirty.
   * (do not confuse with reset which is moving back to a specified revision - without dirty)
   */
  @Override
  public Response<CollaborationMergeChange> revertItemVersionRevision(SessionContext context,
                                                                      Id itemId, Id versionId,
                                                                      Id revisionId) {
    Optional<ItemVersion> itemVersion = getItemVersion(context, itemId, versionId, revisionId);
    if (!itemVersion.isPresent()) {
      throw new IllegalArgumentException(String
          .format("Item %s, version %s: Cannot revert to revision %s since it is not found", itemId,
              versionId, revisionId));
    }

    // TODO: 12/4/2017 force sync is done in order to clear dirty element on private
    // this is temp solution that should be fixed.
    forceSyncItemVersion(context, itemId, versionId);

    revertService.revert(context, itemId, versionId, revisionId);

    return new Response<>(new CollaborationMergeChange());
  }

  @Override
  public Response<Void> commitElements(SessionContext context, Id itemId, Id versionId, String s) {
    // not needed
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Collection<CollaborationElement>> listElements(SessionContext context,
                                                                 ElementContext elementContext,
                                                                 Namespace namespace,
                                                                 Id elementId) {
    return new Response<>(elementPrivateStore.listSubs(context, elementContext, elementId).stream()
        .map(elementEntity -> ZusammenPluginUtil
            .convertToCollaborationElement(elementContext, elementEntity))
        .collect(Collectors.toList()));
  }

  @Override
  public Response<CollaborationElement> getElement(SessionContext context,
                                                   ElementContext elementContext,
                                                   Namespace namespace, Id elementId) {
    return new Response<>(elementPrivateStore.get(context, elementContext, elementId)
        .map(elementEntity -> ZusammenPluginUtil
            .convertToCollaborationElement(elementContext, elementEntity))
        .orElse(null));
  }

  @Override
  public Response<CollaborationElementConflict> getElementConflict(SessionContext context,
                                                                   ElementContext elementContext,
                                                                   Namespace namespace,
                                                                   Id elementId) {
    Optional<StageEntity<ElementEntity>> conflictedStagedElement =
        elementStageStore
            .getConflicted(context, elementContext, new ElementEntity(elementId));

    return new Response<>(conflictedStagedElement
        .map(stagedElement -> getElementConflict(context, elementContext, stagedElement))
        .orElse(null));
  }

  @Override
  public Response<Void> createElement(SessionContext context, CollaborationElement element) {
    elementPrivateStore.create(context,
        new ElementContext(element.getItemId(), element.getVersionId()),
        ZusammenPluginUtil.convertToElementEntity(element));
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> updateElement(SessionContext context, CollaborationElement element) {
    elementPrivateStore.update(context,
        new ElementContext(element.getItemId(), element.getVersionId()),
        ZusammenPluginUtil.convertToElementEntity(element));
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> deleteElement(SessionContext context, CollaborationElement element) {
    elementPrivateStore
        .delete(context, new ElementContext(element.getItemId(), element.getVersionId()),
            ZusammenPluginUtil.convertToElementEntity(element));

    return new Response(Void.TYPE);
  }

  @Override
  public Response<CollaborationMergeResult> resolveElementConflict(SessionContext context,
                                                                   CollaborationElement element,
                                                                   Resolution resolution) {
    ElementContext elementContext = new ElementContext(element.getItemId(), element.getVersionId());
    elementStageStore
        .resolveConflict(context, elementContext,
            ZusammenPluginUtil.convertToElementEntity(element), resolution);
    commitStagingService.commitStaging(context, element.getItemId(), element.getVersionId());

    return new Response<>(new CollaborationMergeResult());
  }

  @Override
  public Response<ItemVersion> getItemVersion(SessionContext context, Space space, Id itemId,
                                              Id versionId, Id revisionId) {
    return new Response<>(getItemVersion(context, itemId, versionId, revisionId).orElse(null));
  }

  @Override
  public Response<HealthInfo> checkHealth(SessionContext context) {
    HealthInfo healthInfo = versionPublicStore.checkHealth(context)
        ? new HealthInfo(Module.ZCSP.getDescription(), HealthStatus.UP, "")
        : new HealthInfo(Module.ZCSP.getDescription(), HealthStatus.DOWN, "No Schema Available");

    return new Response<>(healthInfo);
  }

  private void discardItemVersionChanges(SessionContext context, Id itemId, Id versionId) {
    discardChangesService.discardChanges(context, itemId, versionId);
    commitStagingService.commitStaging(context, itemId, versionId);
  }

  private Optional<ItemVersion> getItemVersion(SessionContext context, Id itemId, Id versionId,
                                               Id revisionId) {
    // since revisions are kept only on public - get from there
    Optional<VersionEntity> versionEntity = versionPublicStore.get(context, itemId, versionId);
    if (!versionEntity.isPresent()) {
      return Optional.empty();
    }

    return elementPublicStore
        .getDescriptor(context, new ElementContext(itemId, versionId, revisionId),
            ROOT_ELEMENTS_PARENT_ID)
        .map(ZusammenPluginUtil::convertToVersionData)
        .map(itemVersionData -> ZusammenPluginUtil
            .convertToItemVersion(versionEntity.get(), itemVersionData));
  }

  private List<ElementEntity> listVersionElements(SessionContext context,
                                                  ElementContext elementContext) {
    return elementPrivateStore.listIds(context, elementContext).entrySet().stream() // TODO:
        // 9/5/2017 parallel
        .map(entry -> elementPrivateStore.get(context, elementContext, entry.getKey()).get())
        .collect(Collectors.toList());
  }

  private void copyElements(SessionContext context,
                            ElementContext sourceContext, ElementContext targetContext) {
    listVersionElements(context, sourceContext).forEach(element -> {
      // publishTime copied as is and dirty is off
      Date publishTime =
          elementPrivateStore.getSynchronizationState(context, sourceContext, element.getId())
              .get().getPublishTime();
      elementPrivateStore.commitStagedCreate(context, targetContext, element, publishTime);
    });
  }

  private ItemVersionDataConflict getVersionDataConflict(SessionContext context,
                                                         ElementContext elementContext,
                                                         StageEntity<ElementEntity> stagedElementDescriptor) {
    ItemVersionDataConflict versionConflict = new ItemVersionDataConflict();
    versionConflict.setRemoteData(
        ZusammenPluginUtil.convertToVersionData(stagedElementDescriptor.getEntity()));
    if (stagedElementDescriptor.getAction() == Action.UPDATE) {
      versionConflict.setLocalData(getPrivateVersionData(context, elementContext));
    }
    return versionConflict;
  }

  private ItemVersionData getPrivateVersionData(SessionContext context,
                                                ElementContext elementContext) {
    return elementPrivateStore
        .getDescriptor(context, elementContext, ROOT_ELEMENTS_PARENT_ID)
        .map(ZusammenPluginUtil::convertToVersionData)
        .orElseThrow(() -> new IllegalStateException("Version must have data"));
  }

  private ElementConflictDescriptor getElementConflictDescriptor(SessionContext context,
                                                                 ElementContext elementContext,
                                                                 StageEntity<ElementEntity> stagedElementDescriptor) {
    ElementDescriptor elementDescriptorFromStage =
        ZusammenPluginUtil
            .convertToElementDescriptor(elementContext, (stagedElementDescriptor.getEntity()));

    ElementConflictDescriptor conflictDescriptor = new ElementConflictDescriptor();
    switch (stagedElementDescriptor.getAction()) {
      case CREATE:
        conflictDescriptor.setRemoteElementDescriptor(elementDescriptorFromStage);
        break;
      case UPDATE:
        conflictDescriptor.setRemoteElementDescriptor(elementDescriptorFromStage);
        conflictDescriptor
            .setLocalElementDescriptor(ZusammenPluginUtil.convertToElementDescriptor(elementContext,
                elementPrivateStore
                    .getDescriptor(context, elementContext,
                        stagedElementDescriptor.getEntity().getId())
                    .orElse(null)));// updated on public while deleted from private
        break;
      case DELETE:
        conflictDescriptor.setLocalElementDescriptor(elementDescriptorFromStage);
        break;
      default:
        break;
    }
    return conflictDescriptor;
  }

  private CollaborationElementConflict getElementConflict(SessionContext context,
                                                          ElementContext entityContext,
                                                          StageEntity<ElementEntity> stagedElement) {
    CollaborationElement elementFromStage =
        ZusammenPluginUtil
            .convertToCollaborationElement(entityContext, (stagedElement.getEntity()));

    CollaborationElementConflict conflict = new CollaborationElementConflict();
    switch (stagedElement.getAction()) {
      case CREATE:
        conflict.setRemoteElement(elementFromStage);
        break;
      case UPDATE:
        conflict.setRemoteElement(elementFromStage);
        conflict.setLocalElement(
            elementPrivateStore.get(context, entityContext, stagedElement.getEntity().getId())
                .map(element -> ZusammenPluginUtil
                    .convertToCollaborationElement(entityContext, element))
                .orElse(null));// updated on public while deleted from private
        break;
      case DELETE:
        conflict.setLocalElement(elementFromStage);
        break;
      default:
        break;
    }
    return conflict;
  }

  private Revision convertSyncStateToRevision(SynchronizationStateEntity syncState) {
    Revision revision = new Revision();
    revision.setRevisionId(syncState.getRevisionId());
    revision.setTime(syncState.getPublishTime());
    revision.setMessage(syncState.getMessage());
    revision.setUser(syncState.getUser());
    return revision;
  }
}