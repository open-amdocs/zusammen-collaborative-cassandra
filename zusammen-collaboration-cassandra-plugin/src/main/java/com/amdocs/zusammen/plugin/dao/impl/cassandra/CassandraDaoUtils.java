package com.amdocs.zusammen.plugin.dao.impl.cassandra;

import com.amdocs.zusammen.commons.db.api.cassandra.CassandraConnectorFactory;
import com.amdocs.zusammen.commons.db.api.cassandra.types.CassandraContext;
import com.amdocs.zusammen.datatypes.SessionContext;
import com.datastax.driver.core.TypeCodec;

class CassandraDaoUtils {

  private CassandraDaoUtils() {
  }

  static <T> T getAccessor(SessionContext context, Class<T> classOfT) {
    return CassandraConnectorFactory.getInstance().createInterface()
        .getMappingManager(getCassandraContext(context))
        .createAccessor(classOfT);
  }

  static void registerCodecs(TypeCodec... codecs) {
    CassandraConnectorFactory.getInstance().createInterface()
        .getConfiguration().getCodecRegistry().register(codecs);
  }

  private static CassandraContext getCassandraContext(SessionContext context) {
    CassandraContext cassandraContext = new CassandraContext();
    cassandraContext.setTenant(context.getTenant());
    return cassandraContext;
  }
}
