package org.apache.sqoop.mapreduce.mainframe;

/********
 * Basic enumeration for Mainframe data set types
 ********/

public enum MainframeDatasetType {
	GDG, PARTITIONED, SEQUENTIAL;
	
	@Override
	  public String toString() {
	    switch(this) {
	      case GDG: return MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_GDG;
	      case PARTITIONED: return MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_PARTITIONED;
	      case SEQUENTIAL: return MainframeConfiguration.MAINFRAME_INPUT_DATASET_TYPE_SEQUENTIAL;
	      default: throw new IllegalArgumentException();
	    }
	  }
}
