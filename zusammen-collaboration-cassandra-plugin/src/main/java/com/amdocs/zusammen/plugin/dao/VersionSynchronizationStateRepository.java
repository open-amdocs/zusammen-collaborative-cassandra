package com.amdocs.zusammen.plugin.dao;

import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;
import com.amdocs.zusammen.plugin.dao.types.VersionContext;
import com.amdocs.zusammen.plugin.dao.types.VersionEntity;

import java.util.List;

public interface VersionSynchronizationStateRepository
    extends SynchronizationStateRepository<VersionContext> {

  void updatePublishTime(SessionContext context, VersionContext entityContext,
                         SynchronizationStateEntity syncStateEntity);

  List<SynchronizationStateEntity> list(SessionContext context, VersionContext entityContext,
                                        VersionEntity versionEntity);
}
