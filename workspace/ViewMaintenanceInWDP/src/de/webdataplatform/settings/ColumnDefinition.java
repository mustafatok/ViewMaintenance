package de.webdataplatform.settings;

public class ColumnDefinition {


		private String name;
		
		private String family;
		
		private String prefix;
		
		private long startRange;
		
		private long endRange;
		

		public long getNumOfValues(){
			return endRange - startRange;
		}
		

		public ColumnDefinition(String name, String family, String prefix,
				long startRange, long endRange) {
			super();
			this.name = name;
			this.family = family;
			this.prefix = prefix;
			this.startRange = startRange;
			this.endRange = endRange;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getFamily() {
			return family;
		}

		public void setFamily(String family) {
			this.family = family;
		}

		public String getPrefix() {
			return prefix;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}

		public long getStartRange() {
			return startRange;
		}

		public void setStartRange(long startRange) {
			this.startRange = startRange;
		}

		public long getEndRange() {
			return endRange;
		}

		public void setEndRange(long endRange) {
			this.endRange = endRange;
		}

		@Override
		public String toString() {
			return "ColumnDefinition [name=" + name + ", family=" + family
					+ ", prefix=" + prefix + ", startRange=" + startRange
					+ ", endRange=" + endRange + "]";
		}
	
	
	

		
		
	

}
