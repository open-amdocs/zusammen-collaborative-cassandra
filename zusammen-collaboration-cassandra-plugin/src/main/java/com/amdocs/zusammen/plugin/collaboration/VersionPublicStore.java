package com.amdocs.zusammen.plugin.collaboration;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.itemversion.ItemVersionRevisions;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.dao.types.VersionEntity;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

public interface VersionPublicStore {

  Collection<VersionEntity> list(SessionContext context, Id itemId);

  Optional<VersionEntity> get(SessionContext context, Id itemId, Id versionId);

  Optional<SynchronizationStateEntity> getSynchronizationState(SessionContext context,
                                                               Id itemId, Id versionId);

  void create(SessionContext context, Id itemId, VersionEntity version, Id revisionId,
              Map<Id, Id> versionElementIds, Date publishTime, String message);

  void update(SessionContext context, Id itemId, VersionEntity version, Id revisionId,
              Map<Id, Id> versionElementIds, Date publishTime, String message);

  void delete(SessionContext context, Id itemId, VersionEntity version);

  ItemVersionRevisions listRevisions(SessionContext context, Id itemId, Id versionId);

  boolean checkHealth(SessionContext context);
}
