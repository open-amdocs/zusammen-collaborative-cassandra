package com.amdocs.zusammen.plugin.dao;

import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.plugin.statestore.cassandra.dao.types.ElementEntityContext;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;

import java.util.Collection;

public interface ElementSynchronizationStateRepository
    extends SynchronizationStateRepository<ElementEntityContext> {

  Collection<SynchronizationStateEntity> list(SessionContext context,
                                              ElementEntityContext elementContext);

  void update(SessionContext context, ElementEntityContext entityContext,
              SynchronizationStateEntity syncStateEntity);

  void markAsDirty(SessionContext context, ElementEntityContext entityContext,
                   SynchronizationStateEntity syncStateEntity);

}
