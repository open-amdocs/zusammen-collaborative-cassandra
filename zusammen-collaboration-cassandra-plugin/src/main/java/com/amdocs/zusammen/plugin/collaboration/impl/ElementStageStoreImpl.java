package com.amdocs.zusammen.plugin.collaboration.impl;

import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.item.Action;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.datatypes.item.Resolution;
import com.amdocs.zusammen.plugin.collaboration.ElementStageStore;
import com.amdocs.zusammen.plugin.dao.ElementStageRepository;
import com.amdocs.zusammen.plugin.dao.ElementStageRepositoryFactory;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;
import com.amdocs.zusammen.plugin.dao.types.StageEntity;
import com.amdocs.zusammen.plugin.statestore.cassandra.dao.types.ElementEntityContext;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amdocs.zusammen.plugin.ZusammenPluginUtil.getPrivateElementContext;
import static com.amdocs.zusammen.plugin.ZusammenPluginUtil.getPrivateSpaceName;

public class ElementStageStoreImpl implements ElementStageStore {

  @Override
  public Collection<ElementEntity> listIds(SessionContext context, ElementContext elementContext) {
    return getElementStageRepository(context)
        .listIds(context, new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext(elementContext)));
  }

  @Override
  public Optional<StageEntity<ElementEntity>> get(SessionContext context,
                                                  ElementContext elementContext,
                                                  ElementEntity element) {
    return getElementStageRepository(context).get(context,
        new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext(elementContext)), element);
  }

  @Override
  public Optional<StageEntity<ElementEntity>> getConflicted(SessionContext context,
                                                            ElementContext elementContext,
                                                            ElementEntity element) {
    return get(context, elementContext, element).filter(StageEntity::isConflicted);
  }

  @Override
  public boolean hasConflicts(SessionContext context, ElementContext elementContext) {
    return !getElementStageRepository(context).listConflictedIds(context,
        new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext(elementContext))).isEmpty();
  }

  @Override
  public Collection<StageEntity<ElementEntity>> listConflictedDescriptors(SessionContext context,
                                                                          ElementContext elementContext) {
    ElementEntityContext privateContext =
        new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext
                (elementContext));
    ElementStageRepository elementStageRepository = getElementStageRepository(context);

    return elementStageRepository.listConflictedIds(context, privateContext).stream()
        .map(conflictedElement -> getConflictedDescriptor(context, privateContext,
            elementStageRepository, conflictedElement))
        .collect(Collectors.toList());
  }

  private StageEntity<ElementEntity> getConflictedDescriptor(SessionContext context,
                                                             ElementEntityContext privateContext,
                                                             ElementStageRepository elementStageRepository,
                                                             ElementEntity conflictedElement) {
    return elementStageRepository.getDescriptor(context, privateContext, conflictedElement)
        .orElseThrow(() -> new IllegalStateException(String.format(
            "Get staged conflicted element error: " +
                "Element %s, which appears as a staged conflicted element of item %s version %s, " +
                "does not exist in stage",
            conflictedElement.getId(), privateContext.getItemId(), privateContext.getVersionId())));
  }

  @Override
  public void deleteAll(SessionContext context, ElementContext elementContext) {
    for (ElementEntity element : listIds(context, elementContext)) {
      StageEntity<ElementEntity> stagedElement =
          get(context, elementContext, element).orElseThrow(
              () -> new IllegalStateException("Staged element id returned by list must exist"));
      delete(context, elementContext, stagedElement.getEntity());
    }
  }

  @Override
  public void create(SessionContext context, ElementContext elementContext,
                     StageEntity<ElementEntity> elementStage) {
    getElementStageRepository(context).create(context,
        new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext(elementContext)), elementStage);
  }

  @Override
  public void delete(SessionContext context, ElementContext elementContext, ElementEntity element) {
    getElementStageRepository(context).delete(context,
        new ElementEntityContext(getPrivateSpaceName(context),
            getPrivateElementContext(elementContext)), element);
  }


  @Override
  public void resolveConflict(SessionContext context, ElementContext elementContext,
                              ElementEntity element, Resolution resolution) {
    Optional<StageEntity<ElementEntity>> stagedElement =
        getConflicted(context, elementContext, element);
    if (!stagedElement.isPresent()) {
      return;
    }

    ElementEntityContext privateContext = new ElementEntityContext(getPrivateSpaceName(context),
        getPrivateElementContext(elementContext));

    switch (resolution) {
      case YOURS:
        resolveConflictByYours(context, privateContext, stagedElement.get());
        break;
      case THEIRS:
        resolveConflictByTheirs(context, privateContext, stagedElement.get());
        break;
      case OTHER: // other = data updates only? no data deletions? if so, then:
        // conflicted = false
        // element = the input of resolve
        // action = update
        throw new UnsupportedOperationException("'Other' conflict resolution is not yet supported");
      default:
        break;
    }
  }

  private void resolveConflictByYours(SessionContext context, ElementEntityContext privateContext,
                                      StageEntity<ElementEntity> stagedElement) {
    getElementStageRepository(context)
        .markAsNotConflicted(context, privateContext, stagedElement.getEntity(), Action.IGNORE);
    stagedElement.getConflictDependents().forEach(conflictDependant ->
        getElementStageRepository(context)
            .markAsNotConflicted(context, privateContext, conflictDependant, Action.IGNORE));
  }

  private void resolveConflictByTheirs(SessionContext context, ElementEntityContext privateContext,
                                       StageEntity<ElementEntity> stagedElement) {
    getElementStageRepository(context)
        .markAsNotConflicted(context, privateContext, stagedElement.getEntity());
  }

  protected ElementStageRepository getElementStageRepository(SessionContext context) {
    return ElementStageRepositoryFactory.getInstance().createInterface(context);
  }
}
