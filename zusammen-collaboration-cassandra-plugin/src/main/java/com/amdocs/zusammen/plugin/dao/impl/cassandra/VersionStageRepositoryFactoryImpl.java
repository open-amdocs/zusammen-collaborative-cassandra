package com.amdocs.zusammen.plugin.dao.impl.cassandra;

import com.amdocs.zusammen.datatypes.SessionContext;
import com.amdocs.zusammen.plugin.dao.VersionStageRepository;
import com.amdocs.zusammen.plugin.dao.VersionStageRepositoryFactory;

public class VersionStageRepositoryFactoryImpl extends VersionStageRepositoryFactory {

  private static final VersionStageRepository INSTANCE = new VersionStageRepositoryImpl();

  @Override
  public VersionStageRepository createInterface(SessionContext context) {
    return INSTANCE;
  }
}
