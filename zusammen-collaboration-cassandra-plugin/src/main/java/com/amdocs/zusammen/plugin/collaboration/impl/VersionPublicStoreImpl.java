package com.amdocs.zusammen.plugin.collaboration.impl;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.Space;
import com.amdocs.zusammen.plugin.collaboration.VersionPublicStore;
import com.amdocs.zusammen.plugin.dao.VersionDao;
import com.amdocs.zusammen.plugin.dao.VersionDaoFactory;
import com.amdocs.zusammen.plugin.dao.VersionSynchronizationStateRepository;
import com.amdocs.zusammen.plugin.dao.VersionSynchronizationStateRepositoryFactory;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.dao.types.VersionContext;
import com.amdocs.zusammen.plugin.dao.types.VersionEntity;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amdocs.zusammen.plugin.ZusammenPluginUtil.getSpaceName;

public class VersionPublicStoreImpl implements VersionPublicStore {

  @Override
  public Collection<VersionEntity> list(SessionContext context, Id itemId) {
    return getVersionDao(context).list(context, getSpaceName(context, Space.PUBLIC), itemId);
  }

  @Override
  public Optional<VersionEntity> get(SessionContext context, Id itemId, Id versionId) {
    return getVersionDao(context)
        .get(context, getSpaceName(context, Space.PUBLIC), itemId, versionId);
  }

  @Override
  public List<SynchronizationStateEntity> listSynchronizationStates(SessionContext context,
                                                                    Id itemId, Id versionId) {
    List<SynchronizationStateEntity> syncStates = getVersionSyncStateRepository(context)
        .list(context, new VersionContext(getSpaceName(context, Space.PUBLIC), itemId),
            new SynchronizationStateEntity(versionId, null));
    syncStates.sort((o1, o2) -> o1.getPublishTime().after(o2.getPublishTime()) ? -1 : 1);
    return syncStates;
  }

  @Override
  public Optional<SynchronizationStateEntity> getSynchronizationState(SessionContext context,
                                                                      Id itemId, Id versionId,
                                                                      Id revisionId) {
    if (revisionId == null) {
      Optional<SynchronizationStateEntity> lastSyncState =
          getLastSynchronizationState(context, itemId, versionId);
      if (!lastSyncState.isPresent()) {
        return Optional.empty();
      }
      revisionId = lastSyncState.get().getRevisionId();
    }

    return getVersionSyncStateRepository(context)
        .get(context, new VersionContext(getSpaceName(context, Space.PUBLIC), itemId),
            new SynchronizationStateEntity(versionId, revisionId));
  }


  @Override
  public void create(SessionContext context, Id itemId, VersionEntity version, Id revisionId,
                     Map<Id, Id> versionElementIds, Date publishTime, String message) {
    String publicSpace = getSpaceName(context, Space.PUBLIC);

    getVersionDao(context).create(context, publicSpace, itemId, version);

    getVersionDao(context).createVersionElements(context, publicSpace, itemId, version.getId(),
        revisionId, versionElementIds, publishTime, message);

    getVersionSyncStateRepository(context).create(context, new VersionContext(publicSpace, itemId),
        new SynchronizationStateEntity(version.getId(), revisionId, publishTime, false));
  }

  @Override
  public void update(SessionContext context, Id itemId, VersionEntity version, Id revisionId,
                     Map<Id, Id> versionElementIds, Date publishTime, String message) {
    String publicSpace = getSpaceName(context, Space.PUBLIC);

    getVersionDao(context).
        createVersionElements(context, publicSpace, itemId, version.getId(),
            revisionId, versionElementIds, publishTime, message);

    getVersionSyncStateRepository(context).
        updatePublishTime(context, new VersionContext(publicSpace, itemId),
            new SynchronizationStateEntity(version.getId(), revisionId, publishTime, false));
  }

  @Override
  public void delete(SessionContext context, Id itemId, VersionEntity version) {
    String publicSpace = getSpaceName(context, Space.PUBLIC);

    getVersionDao(context).delete(context, publicSpace, itemId, version.getId());
    getVersionSyncStateRepository(context)
        .delete(context, new VersionContext(publicSpace, itemId),
            new SynchronizationStateEntity(version.getId(), null));
  }

  @Override
  public boolean checkHealth(SessionContext context) {
    return getVersionDao(context).checkHealth(context);
  }

  private Optional<SynchronizationStateEntity> getLastSynchronizationState(SessionContext context,
                                                                           Id itemId,
                                                                           Id versionId) {
    List<SynchronizationStateEntity> syncStates =
        listSynchronizationStates(context, itemId, versionId);
    return syncStates.isEmpty() ?
        Optional.empty() :
        Optional.of(syncStates.get(0));
  }

  protected VersionDao getVersionDao(SessionContext context) {
    return VersionDaoFactory.getInstance().createInterface(context);
  }

  protected VersionSynchronizationStateRepository getVersionSyncStateRepository(
      SessionContext context) {
    return VersionSynchronizationStateRepositoryFactory.getInstance().createInterface(context);
  }
}
