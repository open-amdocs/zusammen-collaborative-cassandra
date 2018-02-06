package com.amdocs.zusammen.plugin.collaboration;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.datatypes.item.ElementContext;
import com.amdocs.zusammen.plugin.dao.types.ElementEntity;

import java.util.Date;
import java.util.Map;

public interface ElementPublicStore extends ElementStore {

  Map<Id,Id> listIds(SessionContext context, ElementContext elementContext);

  void cleanAll(SessionContext context, ElementContext elementContext);

  void create(SessionContext context, ElementContext elementContext, ElementEntity element,
              Date publishTime);

  void update(SessionContext context, ElementContext elementContext, ElementEntity element,
              Date publishTime);

  void delete(SessionContext context, ElementContext elementContext, ElementEntity element,
              Date publishTime);
}
