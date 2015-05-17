package de.webdataplatform.settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

public class JoinNode {


	
	
	private MatchingRow matchingRow;
	
	private List<JoinNode> rightJoinNodes;

	
	
	public JoinNode(MatchingRow leftJoinRow, List<JoinNode> rightJoinNodes) {
		super();
		this.matchingRow = leftJoinRow;
		this.rightJoinNodes = rightJoinNodes;
	}
	
	public void addJoinNode(JoinNode joinNode){
		
		this.rightJoinNodes.add(joinNode);
		
	}
	


	
	public void returnMatchingRow(List<List<MatchingRow>> completeRecords, List<MatchingRow> selectedRecord){
		
		
		if(rightJoinNodes != null && rightJoinNodes.size() != 0){
			
		
				for (JoinNode joinNode : rightJoinNodes) {
					
					
					List<MatchingRow> newList= new ArrayList<MatchingRow>();
					
//					System.out.println("selectedRow: "+selectedRecord);
					
					if(selectedRecord != null){

						for (MatchingRow matchingRow : selectedRecord) {
							newList.add(matchingRow.copy());
						}
						
					}
					newList.add(matchingRow);
					
					joinNode.returnMatchingRow(completeRecords, newList);
		
				}	
		}else{
			
			if(selectedRecord == null)return;
			selectedRecord.add(matchingRow);
			completeRecords.add(selectedRecord);
			
			
		}
		
		
		
	
		
		
	}
	
	public static void main(String[] args) {

		
		Map<byte[], byte[]> cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow11 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("XXX"),cols);
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRowX1 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("k001"),cols);
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRowX2 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("k008"),cols);
		
		JoinNode rootNode = new JoinNode(matchingRow11, new ArrayList<JoinNode>());
		
		JoinNode joinNode1 = new JoinNode(matchingRowX1, new ArrayList<JoinNode>());
		JoinNode joinNode2 = new JoinNode(matchingRowX2, new ArrayList<JoinNode>());
		
		rootNode.addJoinNode(joinNode1);
		rootNode.addJoinNode(joinNode2);
		
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow1 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("x013"),cols);
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow2 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("x001"),cols);
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow3 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("x012"),cols);
		
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow4 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("x015"),cols);
		
		
		cols = new HashMap<byte[], byte[]>();
		cols.put(Bytes.toBytes("colAddKey"), Bytes.toBytes("x029"));
		cols.put(Bytes.toBytes("colAddVal"), Bytes.toBytes("x029"));
		MatchingRow matchingRow5 = new MatchingRow(Bytes.toBytes("btx"), Bytes.toBytes("x099"),cols);
		
		
		JoinNode joinNode3 = new JoinNode(matchingRow1, new ArrayList<JoinNode>());
		JoinNode joinNode4 = new JoinNode(matchingRow2, new ArrayList<JoinNode>());
		JoinNode joinNode5 = new JoinNode(matchingRow3, new ArrayList<JoinNode>());
		JoinNode joinNode6 = new JoinNode(matchingRow4, new ArrayList<JoinNode>());
		JoinNode joinNode7 = new JoinNode(matchingRow5, new ArrayList<JoinNode>());
		
		joinNode1.addJoinNode(joinNode3);
		joinNode1.addJoinNode(joinNode4);
		joinNode1.addJoinNode(joinNode5);
		
		joinNode2.addJoinNode(joinNode6);
		joinNode2.addJoinNode(joinNode7);
		
		
		JoinNode joinNode8 = new JoinNode(matchingRow4, new ArrayList<JoinNode>());
		JoinNode joinNode9 = new JoinNode(matchingRow5, new ArrayList<JoinNode>());		
		
		joinNode7.addJoinNode(joinNode8);
		joinNode7.addJoinNode(joinNode9);
		
		
		List<List<MatchingRow>> result = new ArrayList<List<MatchingRow>>();

		rootNode.returnMatchingRow(result, null);
		
		for (List<MatchingRow> list : result) {
			
			for (MatchingRow matchingRowX : list) {
				System.out.print(matchingRowX);
			}
			System.out.println();
		}
		
		
		
	}
	
	
	
	
	
	

	public MatchingRow getMatchingRow() {
		return matchingRow;
	}

	public void setMatchingRow(MatchingRow leftJoinRow) {
		this.matchingRow = leftJoinRow;
	}

	public List<JoinNode> getRightJoinNodes() {
		return rightJoinNodes;
	}

	public void setRightJoinNodes(List<JoinNode> rightJoinNodes) {
		this.rightJoinNodes = rightJoinNodes;
	}


	

}
