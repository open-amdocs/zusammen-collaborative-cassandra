package com.amdocs.zusammen.plugin.collaboration.impl;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.plugin.ZusammenPluginConstants;
import com.amdocs.zusammen.plugin.collaboration.ElementPrivateStore;
import com.amdocs.zusammen.plugin.dao.ElementRepository;
import com.amdocs.zusammen.plugin.dao.ElementRepositoryFactory;
import com.amdocs.zusammen.plugin.dao.ElementSynchronizationStateRepository;
import com.amdocs.zusammen.plugin.dao.ElementSynchronizationStateRepositoryFactory;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.statestore.cassandra.dao.types.ElementEntityContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amdocs.zusammen.plugin.ZusammenPluginUtil.getPrivateElementContext;
import static com.amdocs.zusammen.plugin.ZusammenPluginUtil.getPrivateSpaceName;

public class ElementPrivateStoreImpl implements ElementPrivateStore {
  private static final Id REVISION_ID = Id.ZERO; // the private revision id is Id.ZERO 0000000...
  private static final String SUB_ELEMENT_NOT_EXIST_ERROR = "Get sub element error: " +
      "Element %s, which appears as a sub element of element %s, " +
      "does not exist in space %s, item %s, version %s";

  @Override
  public Map<Id, Id> listIds(SessionContext context, ElementContext elementContext) {
    return getElementRepository(context)
        .listIds(context, new ElementEntityContext(getPrivateSpaceName(context), elementContext));
  }

  @Override
  public void cleanAll(SessionContext context, ElementContext elementContext) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);

    ElementSynchronizationStateRepository elementSyncStateRepository =
        getElementSyncStateRepository(context);

    ElementRepository elementRepository = getElementRepository(context);
    elementSyncStateRepository
        .list(context, privateContext) // in order to find deleted elements as well
        .forEach(syncState -> elementRepository
            .cleanAllRevisions(context, privateContext, new ElementEntity(syncState.getId())));

    elementSyncStateRepository.deleteAll(context, privateContext);
  }

  @Override
  public Collection<ElementEntity> listSubs(SessionContext context, ElementContext elementContext,
                                            Id elementId) {
    if (elementId == null) {
      elementId = ZusammenPluginConstants.ROOT_ELEMENTS_PARENT_ID;
    }

    ElementRepository elementRepository = getElementRepository(context);
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);

    List<ElementEntity> subElements = new ArrayList<>();
    Optional<Set<Id>> subElementIds = elementRepository
        .get(context, privateContext, new ElementEntity(elementId))
        .map(ElementEntity::getSubElementIds);
    if (!subElementIds.isPresent()) {
      return subElements;
    }

    String elementIdValue = elementId.getValue();
    for (Id subElementId : subElementIds.get()) {
      subElements.add(elementRepository
          .get(context, privateContext, new ElementEntity(subElementId))
          .orElseThrow(() -> new IllegalStateException(String
              .format(SUB_ELEMENT_NOT_EXIST_ERROR, subElementId, elementIdValue,
                  privateContext.getSpace(), privateContext.getItemId(),
                  privateContext.getVersionId()))));
    }
    return subElements;
  }

  @Override
  public Optional<ElementEntity> get(SessionContext context, ElementContext elementContext,
                                     Id elementId) {
    ElementEntityContext privateElementContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateElementContext.setRevisionId(REVISION_ID);
    return getElementRepository(context)
        .get(context, privateElementContext,
            new ElementEntity(elementId));
  }

  @Override
  public Optional<ElementEntity> getDescriptor(SessionContext context,
                                               ElementContext elementContext, Id elementId) {
    return getElementRepository(context)
        .getDescriptor(context,
            new ElementEntityContext(getPrivateSpaceName(context), getPrivateElementContext
                (elementContext)),
            new ElementEntity(elementId));
  }

  @Override
  public Collection<SynchronizationStateEntity> listSynchronizationStates(SessionContext context,
                                                                          ElementContext elementContext) {
    return getElementSyncStateRepository(context)
        .list(context, new ElementEntityContext(getPrivateSpaceName(context), elementContext));
  }

  @Override
  public Optional<SynchronizationStateEntity> getSynchronizationState(SessionContext context,
                                                                      ElementContext elementContext,
                                                                      Id elementId) {

    ElementEntityContext privateElementContext =
        new ElementEntityContext(getPrivateSpaceName(context), getPrivateElementContext
            (elementContext));
    return getElementSyncStateRepository(context)
        .get(context, privateElementContext,
            new SynchronizationStateEntity(elementId, REVISION_ID));
  }

  @Override
  public void create(SessionContext context, ElementContext elementContext, ElementEntity element) {
    create(context, elementContext, element, true, null);
  }

  @Override
  public boolean update(SessionContext context, ElementContext elementContext,
                        ElementEntity element) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);

    if (!isElementChanged(context, privateContext, element)) {
      return false;
    }

    getElementRepository(context).update(context, privateContext, element);
    getElementSyncStateRepository(context).markAsDirty(context, privateContext,
        new SynchronizationStateEntity(element.getId(), REVISION_ID));
    return true;
  }

  @Override
  public void delete(SessionContext context, ElementContext elementContext, ElementEntity element) {

    ElementEntityContext privateElementContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateElementContext.setRevisionId(REVISION_ID);
    deleteElementHierarchy(context, getElementRepository(context),
        getElementSyncStateRepository(context),
        privateElementContext, element);
  }

  @Override
  public void markAsPublished(SessionContext context, ElementContext elementContext, Id elementId,
                              Date publishTime) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);
    getElementSyncStateRepository(context).update(context,
        privateContext,
        new SynchronizationStateEntity(elementId, REVISION_ID, publishTime, false));
  }

  @Override
  public void markDeletionAsPublished(SessionContext context, ElementContext elementContext,
                                      Id elementId, Date publishTime) {

    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);
    getElementSyncStateRepository(context).delete(context,
        privateContext,
        new SynchronizationStateEntity(elementId, REVISION_ID));
  }

  @Override
  public void commitStagedCreate(SessionContext context, ElementContext elementContext,
                                 ElementEntity element, Date publishTime) {
    create(context, elementContext, element, false, publishTime);
  }

  @Override
  public void commitStagedUpdate(SessionContext context, ElementContext elementContext,
                                 ElementEntity element, Date publishTime) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);

    getElementRepository(context).update(context, privateContext, element);
    // Currently Resolution='Other' is not supported so this is invoked after conflict was
    // resolved with Resolution='Theirs' so dirty flag should be turned off.
    // (if there was no conflict it's off anyway)
    getElementSyncStateRepository(context).update(context, privateContext,
        new SynchronizationStateEntity(element.getId(), REVISION_ID, publishTime, false));
  }

  @Override
  public void commitStagedDelete(SessionContext context, ElementContext elementContext,
                                 ElementEntity element) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);
    getElementRepository(context).delete(context, privateContext, element);
    getElementSyncStateRepository(context)
        .delete(context, privateContext,
            new SynchronizationStateEntity(element.getId(), REVISION_ID));
  }

  @Override
  public void commitStagedIgnore(SessionContext context, ElementContext elementContext,
                                 ElementEntity element, Date publishTime) {
    // publish time - updated to mark that this element was already synced with this publish time
    // (even though the local data was preferred) and to prevent this conflict again.
    // dirty - turned on because the local data which is different than the public one was
    // preferred. It will enable future publication of this data.
    getElementSyncStateRepository(context).update(context,
        new ElementEntityContext(getPrivateSpaceName(context), elementContext),
        new SynchronizationStateEntity(element.getId(), REVISION_ID, publishTime, true));
  }

  private void create(SessionContext context, ElementContext elementContext,
                      ElementEntity element, boolean dirty, Date publishTime) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context), elementContext);
    privateContext.setRevisionId(REVISION_ID);
    getElementRepository(context).create(context, privateContext, element);
    getElementSyncStateRepository(context).create(context, privateContext,
        new SynchronizationStateEntity(element.getId(), REVISION_ID, publishTime, dirty));
  }


  private void deleteElementHierarchy(
      SessionContext context, ElementRepository elementRepository,
      ElementSynchronizationStateRepository elementSyncStateRepository,
      ElementEntityContext elementContext, ElementEntity element) {

    Optional<ElementEntity> retrieved = elementRepository.get(context, elementContext, element);
    if (!retrieved.isPresent()) {
      return;
    }
    retrieved.get().getSubElementIds().stream()
        .map(ElementEntity::new)
        .forEach(subElementEntity -> deleteElementHierarchy(
            context, elementRepository, elementSyncStateRepository, elementContext,
            subElementEntity));

    // only for the first one the parentId will populated (so it'll be removed from its parent)
    elementRepository.delete(context, elementContext, element);
    handleDeletedElementSyncState(context, elementSyncStateRepository, elementContext, element);
  }

  private void handleDeletedElementSyncState(SessionContext context,
                                             ElementSynchronizationStateRepository elementSyncStateRepository,
                                             ElementEntityContext elementContext,
                                             ElementEntity element) {
    SynchronizationStateEntity elementSyncState = new SynchronizationStateEntity(element.getId(),
        REVISION_ID);
    if (elementSyncStateRepository.get(context, elementContext, elementSyncState).
        orElseThrow(
            () -> new IllegalStateException("Synchronization state must exist for an element"))
        .getPublishTime() == null) {
      elementSyncStateRepository.delete(context, elementContext, elementSyncState);
    } else {
      elementSyncStateRepository.markAsDirty(context, elementContext, elementSyncState);
    }
  }

  private boolean isElementChanged(SessionContext context,
                                   ElementEntityContext elementContext,
                                   ElementEntity newElement) {
    return getElementHash(context, elementContext, new ElementEntity(newElement.getId()))
        .map(existingHash -> !newElement.getElementHash().equals(existingHash))
        .orElse(true);
  }

  private Optional<Id> getElementHash(SessionContext context,
                                      ElementEntityContext elementEntityContext,
                                      ElementEntity element) {
    return getElementRepository(context).getHash(context, elementEntityContext, element);
  }

  protected ElementRepository getElementRepository(SessionContext context) {
    return ElementRepositoryFactory.getInstance().createInterface(context);
  }

  protected ElementSynchronizationStateRepository getElementSyncStateRepository(
      SessionContext context) {
    return ElementSynchronizationStateRepositoryFactory.getInstance().createInterface(context);
  }

}
