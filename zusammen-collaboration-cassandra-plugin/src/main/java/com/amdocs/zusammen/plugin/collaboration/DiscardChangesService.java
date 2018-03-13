package com.amdocs.zusammen.plugin.collaboration;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.item.Action;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;
import com.amdocs.zusammen.plugin.dao.types.StageEntity;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DiscardChangesService {

  private static final String DISCARD_CHANGES_OF_UNPUBLISHED_VERSION =
      "Item Id %s, version Id %s: Changes of unpublished version cannot be discarded";
  private static final String PRIVATE_VERSION_REVISION_WAS_NOT_FOUND_ON_PUBLIC =
      "Item Id %s, version Id %s: " +
          "private version revision (publish time %s) was not found on public";
  private static final String PUBLIC_SYNC_STATE_EXISTS_WITHOUT_ELEMENT =
      "Item Id %s, version Id %s: Sync state of element with Id %s " +
          "exists in public space while the element does not";
  private static final String PRIVATE_UNPUBLISHED_SYNC_STATE_EXISTS_WITHOUT_ELEMENT =
      "Item Id %s, version Id %s: Sync state of unpublished element with Id %s " +
          "exists in private space while the element does not";

  private VersionPublicStore versionPublicStore;
  private VersionPrivateStore versionPrivateStore;
  private ElementPublicStore elementPublicStore;
  private ElementPrivateStore elementPrivateStore;
  private ElementStageStore elementStageStore;

  public DiscardChangesService(VersionPublicStore versionPublicStore,
                               VersionPrivateStore versionPrivateStore,
                               ElementPublicStore elementPublicStore,
                               ElementPrivateStore elementPrivateStore,
                               ElementStageStore elementStageStore) {
    this.versionPublicStore = versionPublicStore;
    this.versionPrivateStore = versionPrivateStore;
    this.elementPublicStore = elementPublicStore;
    this.elementPrivateStore = elementPrivateStore;
    this.elementStageStore = elementStageStore;
  }

  public void discardChanges(SessionContext context, Id itemId, Id versionId) {

    Optional<SynchronizationStateEntity> privateVersionSyncState =
        versionPrivateStore.getSynchronizationState(context, itemId, versionId);
    if (!privateVersionSyncState.isPresent()) {
      return;
    }

    if (privateVersionSyncState.get().getPublishTime() == null) {
      throw new UnsupportedOperationException(
          String.format(DISCARD_CHANGES_OF_UNPUBLISHED_VERSION, itemId, versionId));
    }

    // TODO: 3/8/2018 done because currently version revision is not stored on private (Zero is stored)
    // when that will be fixed - privateVersionSyncState.get().getRevisionId() can be used instead
    Id revisionIdOfPrivateLastSync = findRevisionIdOfPrivateLastSync(context, itemId, versionId,
        privateVersionSyncState.get().getPublishTime());

    overrideDirtyElements(context,
        new ElementContext(itemId, versionId, revisionIdOfPrivateLastSync));
  }

  private void overrideDirtyElements(SessionContext context, ElementContext elementContext) {
    Map<Id, SynchronizationStateEntity> publicSyncStateById =
        elementPublicStore.listSynchronizationStates(context, elementContext).stream()
            .collect(Collectors.toMap(SynchronizationStateEntity::getId, Function.identity()));

    List<SynchronizationStateEntity> dirtyPrivateSyncStates =
        elementPrivateStore.listSynchronizationStates(context, elementContext).stream()
            .filter(SynchronizationStateEntity::isDirty)
            .collect(Collectors.toList());

    for (SynchronizationStateEntity privateSyncState : dirtyPrivateSyncStates) {
      Optional<ElementEntity> privateElement =
          elementPrivateStore.get(context, elementContext, privateSyncState.getId());
      if (privateSyncState.getPublishTime() == null) {
        stageElement(context, elementContext, privateElement.orElseThrow(
            () -> new IllegalStateException(String
                .format(PRIVATE_UNPUBLISHED_SYNC_STATE_EXISTS_WITHOUT_ELEMENT,
                    elementContext.getItemId(), elementContext.getVersionId(),
                    privateSyncState.getId()))), null, Action.DELETE);
      } else {
        SynchronizationStateEntity publicSyncState =
            publicSyncStateById.get(privateSyncState.getId());
        if (publicSyncState != null) {
          ElementEntity publicElement =
              elementPublicStore.get(context, elementContext, privateSyncState.getId()).orElseThrow(
                  () -> new IllegalStateException(String
                      .format(PUBLIC_SYNC_STATE_EXISTS_WITHOUT_ELEMENT, elementContext.getItemId(),
                          elementContext.getVersionId(), privateSyncState.getId())));

          stageElement(context, elementContext, publicElement, publicSyncState.getPublishTime(),
              privateElement.isPresent() ? Action.UPDATE : Action.CREATE);
        } else {
          stageElement(context, elementContext,
              privateElement.orElseGet(() -> new ElementEntity(privateSyncState.getId())), null,
              Action.DELETE);
        }
      }
    }
  }

  private void stageElement(SessionContext context, ElementContext elementContext,
                            ElementEntity element, Date publishTime, Action action) {
    elementStageStore
        .create(context, elementContext, new StageEntity<>(element, publishTime, action, false));
  }

  private Id findRevisionIdOfPrivateLastSync(SessionContext context, Id itemId, Id versionId,
                                             Date publishTimeOfPrivateLastSync) {
    return versionPublicStore.listSynchronizationStates(context, itemId, versionId).stream()
        .filter(syncState -> publishTimeOfPrivateLastSync.equals(syncState.getPublishTime()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException(
            String.format(PRIVATE_VERSION_REVISION_WAS_NOT_FOUND_ON_PUBLIC, itemId, versionId,
                publishTimeOfPrivateLastSync)))
        .getRevisionId();
  }
}
