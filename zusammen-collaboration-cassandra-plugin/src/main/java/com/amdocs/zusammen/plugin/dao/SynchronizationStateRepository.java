package com.amdocs.zusammen.plugin.dao;

import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;

import java.util.Optional;

public interface SynchronizationStateRepository<C> {

  void create(SessionContext context, C entityContext, SynchronizationStateEntity syncStateEntity);

  void delete(SessionContext context, C entityContext, SynchronizationStateEntity syncStateEntity);

  Optional<SynchronizationStateEntity> get(SessionContext context, C entityContext,
                                           SynchronizationStateEntity syncStateEntity);
}
