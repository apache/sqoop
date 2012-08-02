package org.apache.sqoop.model;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestMPersistableEntity {

  @Test
  public void testPersistableId() {
    PersistentId id = new PersistentId();

    assertFalse(id.hasPersistenceId());

    id.setPersistenceId(666);
    assertTrue(id.hasPersistenceId());
    assertEquals(666, id.getPersistenceId());
  }

  /**
   * Testing class extending MPersistableEntity.
   *
   * Empty implementation with purpose to just test methods available
   * directly in the abstract class.
   */
  public static class PersistentId extends MPersistableEntity {
    @Override
    public String toString() {
      return null;
    }
  }

}
