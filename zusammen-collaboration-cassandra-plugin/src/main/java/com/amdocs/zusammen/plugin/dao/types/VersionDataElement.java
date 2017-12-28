package com.amdocs.zusammen.plugin.dao.types;

import com.amdocs.zusammen.datatypes.Id;
import com.amdocs.zusammen.datatypes.item.ItemVersionData;
import com.amdocs.zusammen.plugin.ZusammenPluginConstants;
import com.amdocs.zusammen.plugin.ZusammenPluginUtil;

public class VersionDataElement extends ElementEntity {

  public VersionDataElement() {
    super(ZusammenPluginConstants.ROOT_ELEMENTS_PARENT_ID);
  }

  public VersionDataElement(ItemVersionData itemVersionData) {
    this();
    setInfo(itemVersionData.getInfo());
    setRelations(itemVersionData.getRelations());
    setElementHash(new Id(ZusammenPluginUtil.calculateElementHash(this)));
  }
}
