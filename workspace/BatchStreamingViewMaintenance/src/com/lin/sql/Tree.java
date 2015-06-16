package com.lin.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple Tree structure implementation
 * @author xiaojielin
 *
 * @param <T>
 */
public class Tree<T> {
    protected Node<T> root;
    
    public Node<T> getRoot() {
		return root;
	}

	public void setRoot(Node<T> root) {
		this.root = root;
	}
	
	public Tree(){
		
	}

	public Tree(T rootData) {
        root = new Node<T>();
        root.data = rootData;
        root.children = new ArrayList<Node<T>>();
    }
	

    public static class Node<T> {
        private T data;
        private Node<T> parent;
        private List<Node<T>> children;
		public T getData() {
			return data;
		}
		public void setData(T data) {
			this.data = data;
		}
		public Node<T> getParent() {
			return parent;
		}
		public void setParent(Node<T> parent) {
			this.parent = parent;
		}
		public List<Node<T>> getChildren() {
			return children;
		}
		public void setChildren(List<Node<T>> children) {
			this.children = children;
		}
        
    }
    
}
