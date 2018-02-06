package com.amdocs.zusammen.plugin.collaboration.impl;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.Space;
import com.amdocs.zusammen.datatypes.itemversion.ItemVersionRevisions;
import com.amdocs.zusammen.datatypes.itemversion.Revision;
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
  public Optional<SynchronizationStateEntity> getSynchronizationState(SessionContext context,
                                                                      Id itemId, Id versionId) {
    Id revisionId = getLastItemVersionRevision(context, itemId, versionId);
    if (revisionId == null) {
      return Optional.empty();
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

  @Override
  public ItemVersionRevisions listRevisions(SessionContext context, Id itemId, Id versionId) {
    List<SynchronizationStateEntity> revisions = getVersionSyncStateRepository(context)
        .list(context, new VersionContext(getSpaceName(context, Space.PUBLIC), itemId),
            new SynchronizationStateEntity(versionId, null));

    if (revisions == null || revisions.isEmpty()) {
      return null;
    }

    revisions.sort((o1, o2) -> o1.getPublishTime().after(o2.getPublishTime()) ? -1 : 1);
    ItemVersionRevisions itemVersionRevisions = new ItemVersionRevisions();
    revisions.forEach(synchronizationStateEntity ->
        itemVersionRevisions.addChange(convertSyncStateToRevision(synchronizationStateEntity)));
    return itemVersionRevisions;
  }

  private Revision convertSyncStateToRevision(SynchronizationStateEntity syncState) {
    Revision revision = new Revision();
    revision.setRevisionId(syncState.getRevisionId());
    revision.setTime(syncState.getPublishTime());
    revision.setMessage(syncState.getMessage());
    revision.setUser(syncState.getUser());
    return revision;
  }

  private Id getLastItemVersionRevision(SessionContext context, Id itemId, Id versionId) {
    ItemVersionRevisions versionRevisions = listRevisions(context, itemId, versionId);
    if (versionRevisions == null) {
      return null;
    }
    return versionRevisions.getItemVersionRevisions().get(0).getRevisionId();
  }

  protected VersionDao getVersionDao(SessionContext context) {
    return VersionDaoFactory.getInstance().createInterface(context);
  }

  protected VersionSynchronizationStateRepository getVersionSyncStateRepository(
      SessionContext context) {
    return VersionSynchronizationStateRepositoryFactory.getInstance().createInterface(context);
  }
}
