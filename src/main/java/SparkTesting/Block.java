package SparkTesting;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class Block implements Serializable {
	
	private Long id;
	
	private Map<Long, IrisRecord> mapOfBlocks;
	
	public Block(Long id, IrisRecord record) {
		Map<Long, IrisRecord> map = Maps.newHashMap();
		map.put(id, record);
		this.mapOfBlocks = map;
		this.id = id;
	}
	
	private Block(Long id, Map<Long, IrisRecord> recordsMap) {
		this.id = id;
		this.mapOfBlocks = recordsMap;
	}
	
	public boolean doesOverlap(Block b) {
		return Sets.intersection(getMergedBlockIds(), b.getMergedBlockIds()).size() > 0;
	}
	
	public List<IrisRecord> getBlock(){
		return Lists.newArrayList(mapOfBlocks.values());
	}
	
	public Long getId(){
		return this.id;
	}
	
	public Map<Long, IrisRecord> getMapOfRecords(){
		return this.mapOfBlocks;
	}
	
	public Set<Long> getMergedBlockIds(){
		return this.mapOfBlocks.keySet();
	}
	
	public boolean contains(Block b) {
		return getMergedBlockIds().containsAll(b.getMergedBlockIds());
	}
	
	public String getRange() {
		List<Long> ids = Lists.newArrayList(getMergedBlockIds());
		Collections.sort(ids);
		Long min = ids.get(0);
		Long max = ids.get(ids.size() - 1);
		Double minValue = mapOfBlocks.get(min).getSepalLength();
		Double maxValue = mapOfBlocks.get(max).getSepalLength();
		return String.format("[%s - %s]", minValue, maxValue);
	}
	
	public Block merge(Block anotherBlock) {
		Long minId = this.getId() <= anotherBlock.getId() ? this.getId(): anotherBlock.getId();
		Map<Long, IrisRecord> map = Maps.newHashMap();
		map.putAll(this.getMapOfRecords());
		map.putAll(anotherBlock.getMapOfRecords());
		return new Block(minId, map);
	}

	@Override
	public String toString() {
		List<Long> ids = Lists.newArrayList(getMergedBlockIds());
		Collections.sort(ids);
		return "Block [id = " + id + ", mergedBlockIds = " + ids + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Block other = (Block) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
}