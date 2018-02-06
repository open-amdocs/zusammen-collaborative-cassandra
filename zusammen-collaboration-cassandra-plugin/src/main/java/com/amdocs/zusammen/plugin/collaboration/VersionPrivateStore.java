package com.amdocs.zusammen.plugin.collaboration;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.dao.types.VersionEntity;

import java.util.Collection;
import java.util.Date;
import java.util.Optional;

public interface VersionPrivateStore {

  Collection<VersionEntity> list(SessionContext context, Id itemId);

  Optional<VersionEntity> get(SessionContext context, Id itemId, Id versionId);

  Optional<SynchronizationStateEntity> getSynchronizationState(SessionContext context, Id itemId,
                                                               Id versionId);

  void create(SessionContext context, Id itemId, VersionEntity version);

  void update(SessionContext context, Id itemId, VersionEntity version);

  void update(SessionContext context, Id itemId, VersionEntity version, Date publishTime,
              boolean dirty);

  void delete(SessionContext context, Id itemId, VersionEntity version);

  void markAsPublished(SessionContext context, Id itemId, Id versionId, Date publishTime);

  void commitStagedCreate(SessionContext context, Id itemId, VersionEntity version,
                          Date publishTime);

  void commitStagedUpdate(SessionContext context, Id itemId, VersionEntity version,
                          Date publishTime);

  void commitStagedIgnore(SessionContext context, Id itemId, VersionEntity version,
                          Date publishTime);


}
