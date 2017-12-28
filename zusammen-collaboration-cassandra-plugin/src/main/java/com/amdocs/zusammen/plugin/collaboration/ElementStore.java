package com.amdocs.zusammen.plugin.collaboration;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;
import com.amdocs.zusammen.plugin.dao.types.SynchronizationStateEntity;

import java.util.Collection;
import java.util.Optional;

public interface ElementStore {
  Optional<ElementEntity> get(SessionContext context, ElementContext elementContext, Id elementId);

  Optional<ElementEntity> getDescriptor(SessionContext context, ElementContext elementContext,
                                        Id elementId);

  Collection<SynchronizationStateEntity> listSynchronizationStates(SessionContext context,
                                                                   ElementContext elementContext);
}
